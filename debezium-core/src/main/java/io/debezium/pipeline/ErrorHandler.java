/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;

public class ErrorHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorHandler.class);

    private final ChangeEventQueue<?> queue;
    private final AtomicReference<Throwable> producerThrowable;
    private final CommonConnectorConfig connectorConfig;
    private final int maxRetries;
    private int retries;

    public ErrorHandler(Class<? extends SourceConnector> connectorType, CommonConnectorConfig connectorConfig,
                        ChangeEventQueue<?> queue) {
        this.connectorConfig = connectorConfig;
        this.queue = queue;
        this.producerThrowable = new AtomicReference<>();
        this.maxRetries = -1;
        this.retries = 0;
    }

    /**
     * Allows a connector that supports setting maximum retries to set the current retries attempts and the maximum retries
     */
    public ErrorHandler(Class<? extends SourceConnector> connectorType, CommonConnectorConfig connectorConfig,
                        ChangeEventQueue<?> queue, int retries, int maxRetries) {
        this.connectorConfig = connectorConfig;
        this.queue = queue;
        this.producerThrowable = new AtomicReference<>();
        this.retries = retries;
        this.maxRetries = maxRetries;
    }

    public void setProducerThrowable(Throwable producerThrowable) {
        LOGGER.error("Producer failure", producerThrowable);

        boolean first = this.producerThrowable.compareAndSet(null, producerThrowable);
        boolean retriable = isRetriable(producerThrowable);

        if (!retriable) {
            retriable = isCustomRetriable(producerThrowable);
        }

        if (first) {
            if (retriable && hasMoreRetries()) {
                queue.producerException(
                        new RetriableException("An exception occurred in the change event producer. This connector will be restarted.", producerThrowable));
            }
            else {
                queue.producerException(new ConnectException("An exception occurred in the change event producer. This connector will be stopped.", producerThrowable));
            }
        }
    }

    public Throwable getProducerThrowable() {
        return producerThrowable.get();
    }

    protected Set<Class<? extends Exception>> communicationExceptions() {
        return Collections.singleton(IOException.class);
    }

    /**
     * Whether the given throwable is retriable (e.g. an exception indicating a
     * connection loss) or not.
     * By default only I/O exceptions are retriable
     */
    protected boolean isRetriable(Throwable throwable) {
        if (throwable == null) {
            return false;
        }
        for (Class<? extends Exception> e : communicationExceptions()) {
            if (e.isAssignableFrom(throwable.getClass())) {
                return true;
            }
        }
        return isRetriable(throwable.getCause());
    }

    /**
     * Whether the given non-retriable matches a custom retriable setting.
     *
     * @return true if non-retriable is converted to retriable
     */
    protected boolean isCustomRetriable(Throwable throwable) {
        if (!connectorConfig.customRetriableException().isPresent()) {
            return false;
        }
        while (throwable != null) {
            if (throwable.getMessage() != null
                    && throwable.getMessage().matches(connectorConfig.customRetriableException().get())) {
                return true;
            }
            throwable = throwable.getCause();
        }
        return false;
    }

    /**
     * Whether the maximum number of retries has been reached
     *
     * @return true if maxRetries is -1 or retries < maxRetries
     */
    protected boolean hasMoreRetries() {
        boolean doRetry = maxRetries == -1 || retries < maxRetries;
        if (doRetry) {
            retries++;
            LOGGER.info("{} of {} retries will be attempted", retries,
                    maxRetries);
        }
        else {
            String errorMsg = String.format(
                    "The maximum number of retries: %d has been attempted", maxRetries);
            LOGGER.error(errorMsg);
        }

        return doRetry;
    }

    public int getRetries() {
        return retries;
    }

    public void resetRetries() {
        this.retries = 0;
    }
}
