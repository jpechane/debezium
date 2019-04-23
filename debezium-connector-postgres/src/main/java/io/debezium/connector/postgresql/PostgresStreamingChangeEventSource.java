/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresReplicationConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.connection.ReplicationStream;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 *
 * @author Horia Chiorean (hchiorea@redhat.com), Jiri Pechanec
 */
public class PostgresStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresStreamingChangeEventSource.class);

    private final PostgresConnection connection;
    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final PostgresSchema schema;
    private final PostgresOffsetContext offsetContext;
    private final Duration pollInterval;
    private final PostgresConnectorConfig connectorConfig;
    private final PostgresTaskContext taskContext;
    private final ReplicationConnection replicationConnection;
    private final AtomicReference<ReplicationStream> replicationStream = new AtomicReference<>();
    private Long lastCompletelyProcessedLsn;

    public PostgresStreamingChangeEventSource(PostgresConnectorConfig connectorConfig, PostgresOffsetContext offsetContext, PostgresConnection connection, EventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock, PostgresSchema schema, PostgresTaskContext taskContext) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.pollInterval = connectorConfig.getPollInterval();
        this.taskContext = taskContext;

        try {
            this.replicationConnection = taskContext.createReplicationConnection();
        } catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        if (!connectorConfig.getSnapshotter().shouldStream()) {
            LOGGER.info("Streaming is not enabled in currect configuration");
            return;
            // TODO Don't start streaming
        }

        try {
            SourceInfo sourceInfo = null;
            if (sourceInfo.hasLastKnownPosition()) {
                // start streaming from the last recorded position in the offset
                Long lsn = sourceInfo.lsn();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("retrieved latest position from stored offset '{}'", ReplicationConnection.format(lsn));
                }
                replicationStream.compareAndSet(null, replicationConnection.startStreaming(lsn));
            } else {
                LOGGER.info("no previous LSN found in Kafka, streaming from the latest xlogpos or flushed LSN...");
                replicationStream.compareAndSet(null, replicationConnection.startStreaming());
            }

            // refresh the schema so we have a latest view of the DB tables
            taskContext.refreshSchema(true);

            this.lastCompletelyProcessedLsn = sourceInfo.lsn();
        }
        catch (Throwable e) {
            errorHandler.setProducerThrowable(e);
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
    }
}
