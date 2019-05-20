/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.ReplicationMessage;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

/**
 * Emits change data based on a logical decoding event coming as protobuf or JSON message.
 *
 * @author Horia Chiorean (hchiorea@redhat.com), Jiri Pechanec
 */
public class PostgresChangeRecordEmitter extends RelationalChangeRecordEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresChangeRecordEmitter.class);

    private final ReplicationMessage message;
    private final PostgresSchema schema;
    private final PostgresConnectorConfig connectorConfig;
    private final PgConnection connection;

    public PostgresChangeRecordEmitter(OffsetContext offset, Clock clock, PostgresConnectorConfig connectorConfig, PostgresSchema schema, PgConnection connection, ReplicationMessage message) {
        super(offset, clock);

        this.schema = schema;
        this.message = message;
        this.connectorConfig = connectorConfig;
        this.connection = connection;
    }

    @Override
    protected Operation getOperation() {
        switch (message.getOperation()) {
        case INSERT: {
            return Operation.CREATE;
        }
        case UPDATE: {
            return Operation.UPDATE;
        }
        case DELETE: {
            return Operation.DELETE;
        }
        default: {
            throw new IllegalArgumentException("Received event of unexpected command type: " + message.getOperation());
        }
    }
    }

    @Override
    protected Object[] getOldColumnValues() {
        final TableId tableId = PostgresSchema.parse(message.getTable());
        Objects.requireNonNull(tableId);

        try {
            switch (getOperation()) {
                case CREATE:
                    return null;
                case UPDATE:
                    return columnValues(message.getOldTupleList(), tableId, true, message.hasTypeMetadata());
                default:
                    return columnValues(message.getOldTupleList(), tableId, true, message.hasTypeMetadata());
            }
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    protected Object[] getNewColumnValues() {
        final TableId tableId = PostgresSchema.parse(message.getTable());
        Objects.requireNonNull(tableId);

        try {
            switch (getOperation()) {
                case CREATE:
                    return columnValues(message.getNewTupleList(), tableId, true, message.hasTypeMetadata());
                case UPDATE:
                    return columnValues(message.getNewTupleList(), tableId, true, message.hasTypeMetadata());
                default:
                    return null;
            }
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    private Object[] columnValues(List<ReplicationMessage.Column> columns, TableId tableId, boolean refreshSchemaIfChanged, boolean metadataInMessage)
            throws SQLException {
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        final Table table = schema.tableFor(tableId);
        Objects.requireNonNull(table);

        // based on the schema columns, create the values on the same position as the columns
        List<Column> schemaColumns = table.columns();
        // JSON does not deliver a list of all columns for REPLICA IDENTITY DEFAULT
        Object[] values = new Object[columns.size() < schemaColumns.size() ? schemaColumns.size() : columns.size()];

        for (ReplicationMessage.Column column : columns) {
            //DBZ-298 Quoted column names will be sent like that in messages, but stored unquoted in the column names
            final String columnName = Strings.unquoteIdentifierPart(column.getName());
            final Column tableColumn = table.columnWithName(columnName);
            if (tableColumn == null) {
                LOGGER.warn(
                        "Internal schema is out-of-sync with incoming decoder events; column {} will be omitted from the change event.",
                        column.getName());
                continue;
            }
            int position = tableColumn.position() - 1;
            if (position < 0 || position >= values.length) {
                LOGGER.warn(
                        "Internal schema is out-of-sync with incoming decoder events; column {} will be omitted from the change event.",
                        column.getName());
                continue;
            }
            values[position] = column.getValue(() -> connection, connectorConfig.includeUnknownDatatypes());
        }

        return values;
    }
}
