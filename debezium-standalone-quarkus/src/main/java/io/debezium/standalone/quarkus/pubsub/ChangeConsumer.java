/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.standalone.quarkus.pubsub;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;

/**
 * Implementation of the consumer that delivers the messages into Google Pub/Sub destination.
 *
 * @author Jiri Pechanec
 *
 */
@ApplicationScoped
@Named("pub-sub")
public class ChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<?, ?>> {

    @Override
    public void handleBatch(List<ChangeEvent<?, ?>> records, RecordCommitter<ChangeEvent<?, ?>> committer)
            throws InterruptedException {
        // TODO Implement
    }

}
