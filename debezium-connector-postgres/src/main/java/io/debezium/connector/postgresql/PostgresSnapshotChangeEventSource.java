/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotChangeRecordEmitter;
import io.debezium.relational.HistorizedRelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;

public class PostgresSnapshotChangeEventSource extends HistorizedRelationalSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresSnapshotChangeEventSource.class);

    private final PostgresConnectorConfig connectorConfig;
    private final PostgresConnection jdbcConnection;
    private final PostgresSchema schema;

    public PostgresSnapshotChangeEventSource(PostgresConnectorConfig connectorConfig, PostgresOffsetContext previousOffset, PostgresConnection jdbcConnection, PostgresSchema schema, EventDispatcher<TableId> dispatcher, Clock clock, SnapshotProgressListener snapshotProgressListener) {
        super(connectorConfig, previousOffset, jdbcConnection, dispatcher, clock, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.schema = schema;
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(OffsetContext previousOffset) {
        boolean snapshotSchema = true;
        boolean snapshotData = true;

        // found a previous offset and the earlier snapshot has completed
        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            LOGGER.info("A previous offset indicating a completed snapshot has been found. Neither schema nor data will be snapshotted.");
            snapshotSchema = false;
            snapshotData = false;
        }
        else {
            LOGGER.info("No previous offset has been found");
            snapshotData = connectorConfig.getSnapshotter().shouldSnapshot();
            if (snapshotData) {
                LOGGER.info("According to the connector configuration data will be snapshotted");
            }
            else {
                LOGGER.info("According to the connector configuration no snapshot will be executed");
            }
        }

        return new SnapshottingTask(snapshotSchema, snapshotData);
    }

    @Override
    protected SnapshotContext prepare(ChangeEventSourceContext context) throws Exception {
        return new PostgresSnapshotContext();
    }

    @Override
    protected void connectionCreated(SnapshotContext snapshotContext) throws Exception {
        LOGGER.info("Setting isolation level");
        // we're using the same isolation level that pg_backup uses
        jdbcConnection.executeWithoutCommitting("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE;");
        schema.refresh(jdbcConnection, false);
    }

    @Override
    protected Set<TableId> getAllTableIds(SnapshotContext ctx) throws Exception {
        return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[] {"TABLE"});
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext) throws SQLException, InterruptedException {
        final long lockTimeoutMillis = connectorConfig.snapshotLockTimeoutMillis();
        final String lineSeparator = System.lineSeparator();
        LOGGER.info("Waiting a maximum of '{}' seconds for each table lock", lockTimeoutMillis / 1000d);
        final StringBuilder statements = new StringBuilder();
        statements.append("SET lock_timeout = ").append(lockTimeoutMillis).append(";").append(lineSeparator);

        // we're locking in SHARE UPDATE EXCLUSIVE MODE to avoid concurrent schema changes while we're taking the snapshot
        // this does not prevent writes to the table, but prevents changes to the table's schema....
        // DBZ-298 Quoting name in case it has been quoted originally; it doesn't do harm if it hasn't been quoted
        snapshotContext.capturedTables.forEach(tableId -> statements.append("LOCK TABLE ")
                                                     .append(tableId.toDoubleQuotedString())
                                                     .append(" IN SHARE UPDATE EXCLUSIVE MODE;")
                                                     .append(lineSeparator));
        jdbcConnection.executeWithoutCommitting(statements.toString());

        //now that we have the locks, refresh the schema
        schema.refresh(jdbcConnection, false);
    }

    @Override
    protected void releaseSchemaSnapshotLocks(SnapshotContext snapshotContext) throws SQLException {
    }

    @Override
    protected void determineSnapshotOffset(SnapshotContext ctx) throws Exception {
        final long lsn = jdbcConnection.currentXLogLocation();
        final long txId = jdbcConnection.currentTransactionId().longValue();
        LOGGER.info("Read xlogStart at '{}' from transaction '{}'", ReplicationConnection.format(lsn), txId);
        ctx.offset = new PostgresOffsetContext(
                connectorConfig.getLogicalName(),
                connectorConfig.databaseName(),
                lsn,
                null,
                txId,
                getClock().currentTimeAsInstant(),
                false,
                false);
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext) throws SQLException, InterruptedException {
        Set<String> schemas = snapshotContext.capturedTables.stream()
                .map(TableId::schema)
                .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            LOGGER.info("Reading structure of schema '{}'", snapshotContext.catalogName);
            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    snapshotContext.catalogName,
                    schema,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false
            );
        }
        schema.refresh(jdbcConnection, false);
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(SnapshotContext snapshotContext, Table table) throws SQLException {
        return new SchemaChangeEvent(snapshotContext.offset.getPartition(), snapshotContext.offset.getOffset(), snapshotContext.catalogName,
                table.id().schema(), null, table, SchemaChangeEventType.CREATE, true);
    }

    @Override
    protected void complete(SnapshotContext snapshotContext) {
    }

    @Override
    protected String getSnapshotSelect(SnapshotContext snapshotContext, TableId tableId) {
        return String.format("SELECT * FROM %s", tableId.toDoubleQuotedString());
    }

    @Override
    protected ChangeRecordEmitter getChangeRecordEmitter(SnapshotContext snapshotContext, Object[] row) {
        ((PostgresOffsetContext) snapshotContext.offset).updateSnapshotPosition(getClock().currentTimeInMicros(), snapshotContext.snapshottedTable);
        return new SnapshotChangeRecordEmitter(snapshotContext.offset, row, getClock());
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class PostgresSnapshotContext extends SnapshotContext {

        public PostgresSnapshotContext() throws SQLException {
            super(null);
        }
    }

}
