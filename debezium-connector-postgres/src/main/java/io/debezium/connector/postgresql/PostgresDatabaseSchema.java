/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Logical representation of PostgreSQL Server schema.
 *
 * @author Jiri Pechanec
 */
public class PostgresDatabaseSchema extends HistorizedRelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDatabaseSchema.class);

    public PostgresDatabaseSchema(PostgresConnectorConfig connectorConfig, SchemaNameAdjuster schemaNameAdjuster, TopicSelector<TableId> topicSelector, SqlServerConnection connection) {
        super(connectorConfig, topicSelector, connectorConfig.getTableFilters().dataCollectionFilter(), connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        new PostgresValueConverter(connectorConfig.getDecimalMode()),
                        schemaNameAdjuster,
                        SourceInfo.SCHEMA
                ),
                false);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        LOGGER.debug("Applying schema change event {}", schemaChange);

        // just a single table per DDL event for PostgreSQL
        Table table = schemaChange.getTables().iterator().next();
        buildAndRegisterSchema(table);
        tables().overwriteTable(table);

        TableChanges tableChanges = null;
        if (schemaChange.getType() == SchemaChangeEventType.CREATE) {
            tableChanges = new TableChanges();
            tableChanges.create(table);
        }
        else if (schemaChange.getType() == SchemaChangeEventType.ALTER) {
            tableChanges = new TableChanges();
            tableChanges.alter(table);
        }


        record(schemaChange, tableChanges);
    }

    @Override
    protected DdlParser getDdlParser() {
        return null;
    }
}
