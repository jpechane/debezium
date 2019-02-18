/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class PostgresChangeEventSourceFactory implements ChangeEventSourceFactory {

    private final PostgresConnectorConfig configuration;
    private final PostgresConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final PostgresDatabaseSchema schema;

    public PostgresChangeEventSourceFactory(PostgresConnectorConfig configuration, PostgresConnection jdbcConnection,
            ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock, PostgresDatabaseSchema schema) {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
    }

    @Override
    public SnapshotChangeEventSource getSnapshotChangeEventSource(OffsetContext offsetContext, SnapshotProgressListener snapshotProgressListener) {
        return new PostgresSnapshotChangeEventSource(configuration, (PostgresOffsetContext) offsetContext, jdbcConnection, schema, dispatcher, clock, snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource getStreamingChangeEventSource(OffsetContext offsetContext) {
        return new PostgresStreamingChangeEventSource(
                configuration,
                (PostgresOffsetContext) offsetContext,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema
        );
    }
}
