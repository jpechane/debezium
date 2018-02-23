/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.internal;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.StopConnectorException;
import io.debezium.embedded.spi.OffsetCommitPolicy;
import io.debezium.util.Clock;
import io.debezium.util.VariableLatch;

/**
 * A base class for of EmbeddedEngine implementations
 *
 * @author Randall Hauch, Jiri Pechanec
 *
 */
@ThreadSafe
public abstract class AbstractEmbeddedEngine implements EmbeddedEngine {

    public static final Field INTERNAL_KEY_CONVERTER_CLASS = Field.create("internal.key.converter")
                                                                     .withDescription("The Converter class that should be used to serialize and deserialize key data for offsets.")
                                                                     .withDefault(JsonConverter.class.getName());

    public static final Field INTERNAL_VALUE_CONVERTER_CLASS = Field.create("internal.value.converter")
                                                                       .withDescription("The Converter class that should be used to serialize and deserialize value data for offsets.")
                                                                       .withDefault(JsonConverter.class.getName());

    /**
     * The array of fields that are required by each connectors.
     */
    public static final Field.Set CONNECTOR_FIELDS = Field.setOf(ENGINE_NAME, CONNECTOR_CLASS);

    /**
     * The array of all exposed fields.
     */
    public static final Field.Set ALL_FIELDS = CONNECTOR_FIELDS.with(OFFSET_STORAGE, OFFSET_STORAGE_FILE_FILENAME,
                                                                        OFFSET_FLUSH_INTERVAL_MS, OFFSET_COMMIT_TIMEOUT_MS,
                                                                        INTERNAL_KEY_CONVERTER_CLASS, INTERNAL_VALUE_CONVERTER_CLASS);

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Configuration config;
    private final Clock clock;
    private final ClassLoader classLoader;
    private final Consumer<SourceRecord> consumer;
    private final CompletionCallback completionCallback;
    private final Optional<ConnectorCallback> connectorCallback;
    private final AtomicReference<Thread> runningThread = new AtomicReference<>();
    private final VariableLatch latch = new VariableLatch(0);
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final WorkerConfig workerConfig;
    private final CompletionResult completionResult;
    private long recordsSinceLastCommit = 0;
    private long timeOfLastCommitMillis = 0;
    private OffsetCommitPolicy offsetCommitPolicy;

    private final String connectorClassName;
    private final long commitTimeoutMs;
    private SourceConnector connector;
    private OffsetStorageWriter offsetWriter;
    private SourceTask task;
    private OffsetBackingStore offsetStore;

    private Throwable handlerError;

    public AbstractEmbeddedEngine(Configuration config, ClassLoader classLoader, Clock clock, Consumer<SourceRecord> consumer,
                           CompletionCallback completionCallback, ConnectorCallback connectorCallback,
                           OffsetCommitPolicy offsetCommitPolicy) {
        this.config = config;
        this.consumer = consumer;
        this.classLoader = classLoader;
        this.clock = clock;
        this.completionCallback = completionCallback != null ? completionCallback : (success, msg, error) -> {
            if (!success) logger.error(msg, error);
        };
        this.connectorCallback = Optional.ofNullable(connectorCallback);
        this.completionResult = new CompletionResult();
        this.offsetCommitPolicy = offsetCommitPolicy;

        assert this.config != null;
        assert this.consumer != null;
        assert this.classLoader != null;
        assert this.clock != null;
        keyConverter = config.getInstance(INTERNAL_KEY_CONVERTER_CLASS, Converter.class, () -> this.classLoader);
        keyConverter.configure(config.subset(INTERNAL_KEY_CONVERTER_CLASS.name() + ".", true).asMap(), true);
        valueConverter = config.getInstance(INTERNAL_VALUE_CONVERTER_CLASS, Converter.class, () -> this.classLoader);
        Configuration valueConverterConfig = config;
        if (valueConverter instanceof JsonConverter) {
            // Make sure that the JSON converter is configured to NOT enable schemas ...
            valueConverterConfig = config.edit().with(INTERNAL_VALUE_CONVERTER_CLASS + ".schemas.enable", false).build();
        }
        valueConverter.configure(valueConverterConfig.subset(INTERNAL_VALUE_CONVERTER_CLASS.name() + ".", true).asMap(), false);

        // Create the worker config, adding extra fields that are required for validation of a worker config
        // but that are not used within the embedded engine (since the source records are never serialized) ...
        Map<String, String> embeddedConfig = config.asMap(ALL_FIELDS);
        embeddedConfig.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        embeddedConfig.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        workerConfig = new EmbeddedConfig(embeddedConfig);

        connectorClassName = config.getString(CONNECTOR_CLASS);
        commitTimeoutMs = config.getLong(OFFSET_COMMIT_TIMEOUT_MS);
    }

    /**
     * Determine if this embedded connector is currently running.
     *
     * @return {@code true} if running, or {@code false} otherwise
     */
    public boolean isRunning() {
        return this.runningThread.get() != null;
    }

    private void fail(String msg) {
        fail(msg, null);
    }

    private void fail(String msg, Throwable error) {
        if (completionResult.hasError()) {
            // there's already a recorded failure, so keep the original one and simply log this one
            logger.error(msg, error);
            return;
        }
        // don't use the completion callback here because we want to store the error and message only
        completionResult.handle(false, msg, error);
    }

    private void succeed(String msg) {
        // don't use the completion callback here because we want to store the error and message only
        completionResult.handle(true, msg, null);
    }

    /**
     * Initialize all engine components and start the connector
     */
    protected void doStart() {
        final String engineName = config.getString(ENGINE_NAME);

        if (!config.validateAndRecord(CONNECTOR_FIELDS, logger::error)) {
            fail("Failed to start connector with invalid configuration (see logs for actual errors)");
            return;
        }

        // Instantiate the connector ...
        try {
            @SuppressWarnings("unchecked")
            Class<? extends SourceConnector> connectorClass = (Class<SourceConnector>) classLoader.loadClass(connectorClassName);
            connector = connectorClass.newInstance();
        } catch (Throwable t) {
            fail("Unable to instantiate connector class '" + connectorClassName + "'", t);
            return;
        }

        // Instantiate the offset store ...
        final String offsetStoreClassName = config.getString(OFFSET_STORAGE);
        try {
            @SuppressWarnings("unchecked")
            Class<? extends OffsetBackingStore> offsetStoreClass = (Class<OffsetBackingStore>) classLoader.loadClass(offsetStoreClassName);
            offsetStore = offsetStoreClass.newInstance();
        } catch (Throwable t) {
            fail("Unable to instantiate OffsetBackingStore class '" + offsetStoreClassName + "'", t);
            return;
        }

        // Initialize the offset store ...
        try {
            offsetStore.configure(workerConfig);
            offsetStore.start();
        } catch (Throwable t) {
            fail("Unable to configure and start the '" + offsetStoreClassName + "' offset backing store", t);
            return;
        }

        // Set up the offset commit policy ...
        if (offsetCommitPolicy == null) {
            offsetCommitPolicy = config.getInstance(EmbeddedEngine.OFFSET_COMMIT_POLICY, OffsetCommitPolicy.class, config);
        }
        // Initialize the connector using a context that does NOT respond to requests to reconfigure tasks ...
        ConnectorContext context = new ConnectorContext() {

            @Override
            public void requestTaskReconfiguration() {
                // Do nothing ...
            }

            @Override
            public void raiseError(Exception e) {
                fail(e.getMessage(), e);
            }

        };
        connector.initialize(context);
        offsetWriter = new OffsetStorageWriter(offsetStore, engineName,
                keyConverter, valueConverter);
        OffsetStorageReader offsetReader = new OffsetStorageReaderImpl(offsetStore, engineName,
                keyConverter, valueConverter);

        // Start the connector with the given properties and get the task configurations ...
        connector.start(config.asMap());
        connectorCallback.ifPresent(ConnectorCallback::connectorStarted);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        Class<? extends Task> taskClass = connector.taskClass();
        try {
            task = (SourceTask) taskClass.newInstance();
        } catch (IllegalAccessException | InstantiationException t) {
            fail("Unable to instantiate connector's task class '" + taskClass.getName() + "'", t);
            return;
        }
        try {
            SourceTaskContext taskContext = () -> offsetReader;
            task.initialize(taskContext);
            task.start(taskConfigs.get(0));
            connectorCallback.ifPresent(ConnectorCallback::taskStarted);
        } catch (Throwable t) {
            // Mask the passwords ...
            Configuration config = Configuration.from(taskConfigs.get(0)).withMaskedPasswords();
            String msg = "Unable to initialize and start connector's task class '" + taskClass.getName() + "' with config: "
                    + config;
            fail(msg, t);
            return;
        }

        recordsSinceLastCommit = 0;
        handlerError = null;
        timeOfLastCommitMillis = clock.currentTimeInMillis();
    }

    /**
     * Stop the task
     */
    protected void doStopTask() {
        logger.debug("Stopping the task and engine");
        task.stop();
        connectorCallback.ifPresent(ConnectorCallback::taskStopped);
        // Always commit offsets that were captured from the source records we actually processed ...
        commitOffsets(offsetWriter, commitTimeoutMs, task);
        if (handlerError == null) {
            // We stopped normally ...
            succeed("Connector '" + connectorClassName + "' completed normally.");
        }
    }

    /**
     * Stop the connector
     */
    protected void doStopConnector() {
        try {
            offsetStore.stop();
        } catch (Throwable t) {
            fail("Error while trying to stop the offset store", t);
        } finally {
            try {
                connector.stop();
                connectorCallback.ifPresent(ConnectorCallback::connectorStopped);
            } catch (Throwable t) {
                fail("Error while trying to stop connector class '" + connectorClassName + "'", t);
            }
        }
    }

    /**
     * Run this embedded connector and deliver database changes to the registered {@link Consumer}. This method blocks until
     * the connector is stopped.
     * <p>
     * First, the method checks to see if this instance is currently {@link #run() running}, and if so immediately returns.
     * <p>
     * If the configuration is valid, this method starts the connector and starts polling the connector for change events.
     * All messages are delivered in batches to the {@link Consumer} registered with this embedded connector. The batch size,
     * polling
     * frequency, and other parameters are controlled via configuration settings. This continues until this connector is
     * {@link #stop() stopped}.
     * <p>
     * Note that there are two ways to stop a connector running on a thread: calling {@link #stop()} from another thread, or
     * interrupting the thread (e.g., via {@link ExecutorService#shutdownNow()}).
     * <p>
     * This method can be called repeatedly as needed.
     */
    @Override
    public void run() {
        if (runningThread.compareAndSet(null, Thread.currentThread())) {
            // Only one thread can be in this part of the method at a time ...
            latch.countUp();

            try {
                doStart();
                if (completionResult.hasError()) {
                    return;
                }
                try {
                    try {
                        timeOfLastCommitMillis = clock.currentTimeInMillis();
                        boolean keepProcessing = true;
                        List<SourceRecord> changeRecords = null;
                        while (runningThread.get() != null && handlerError == null && keepProcessing) {
                            try {
                                try {
                                    logger.debug("Embedded engine is polling task for records on thread " + runningThread.get());
                                    changeRecords = task.poll(); // blocks until there are values ...
                                    logger.debug("Embedded engine returned from polling task for records");
                                } catch (InterruptedException e) {
                                    // Interrupted while polling ...
                                    logger.debug("Embedded engine interrupted on thread " + runningThread.get() + " while polling the task for records");
                                    Thread.interrupted();
                                    break;
                                }
                                try {
                                    if (changeRecords != null && !changeRecords.isEmpty()) {
                                        logger.debug("Received {} records from the task", changeRecords.size());

                                        // First forward the records to the connector's consumer ...
                                        for (SourceRecord record : changeRecords) {
                                            try {
                                                consumer.accept(record);
                                                task.commitRecord(record);
                                            } catch (StopConnectorException e) {
                                                keepProcessing = false;
                                                // Stop processing any more but first record the offset for this record's
                                                // partition
                                                offsetWriter.offset(record.sourcePartition(), record.sourceOffset());
                                                recordsSinceLastCommit += 1;
                                                break;
                                            } catch (Throwable t) {
                                                handlerError = t;
                                                break;
                                            }

                                            // Record the offset for this record's partition
                                            offsetWriter.offset(record.sourcePartition(), record.sourceOffset());
                                            recordsSinceLastCommit += 1;
                                        }

                                        // Flush the offsets to storage if necessary ...
                                        maybeFlush(offsetWriter, offsetCommitPolicy, commitTimeoutMs, task);
                                    } else {
                                        logger.debug("Received no records from the task");
                                    }
                                } catch (Throwable t) {
                                    // There was some sort of unexpected exception, so we should stop work
                                    if (handlerError == null) {
                                        // make sure we capture the error first so that we can report it later
                                        handlerError = t;
                                    }
                                    break;
                                }
                            } finally {
                                // then try to commit the offsets, since we record them only after the records were handled
                                // by the consumer ...
                                maybeFlush(offsetWriter, offsetCommitPolicy, commitTimeoutMs, task);
                            }
                        }
                    } finally {
                        if (handlerError != null) {
                            // There was an error in the handler so make sure it's always captured...
                            fail("Stopping connector after error in the application's handler method: " + handlerError.getMessage(),
                                 handlerError);
                        }
                        try {
                            doStopTask();
                        } catch (Throwable t) {
                            fail("Error while trying to stop the task and commit the offsets", t);
                        }
                    }
                } catch (Throwable t) {
                    fail("Error while trying to run connector class '" + connectorClassName + "'", t);
                } finally {
                    doStopConnector();
                }
            } finally {
                latch.countDown();
                runningThread.set(null);
                // after we've "shut down" the engine, fire the completion callback based on the results we collected
                completionCallback.handle(completionResult.success(), completionResult.message(), completionResult.error());
            }
        }
    }

    /**
     * Determine if we should flush offsets to storage, and if so then attempt to flush offsets.
     *
     * @param offsetWriter the offset storage writer; may not be null
     * @param policy the offset commit policy; may not be null
     * @param commitTimeoutMs the timeout to wait for commit results
     * @param task the task which produced the records for which the offsets have been committed
     */
    protected void maybeFlush(OffsetStorageWriter offsetWriter, OffsetCommitPolicy policy, long commitTimeoutMs,
                              SourceTask task) {
        // Determine if we need to commit to offset storage ...
        long timeSinceLastCommitMillis = clock.currentTimeInMillis() - timeOfLastCommitMillis;
        if (policy.performCommit(recordsSinceLastCommit, Duration.ofMillis(timeSinceLastCommitMillis))) {
            commitOffsets(offsetWriter, commitTimeoutMs, task);
        }
    }

    /**
     * Flush offsets to storage.
     *
     * @param offsetWriter the offset storage writer; may not be null
     * @param commitTimeoutMs the timeout to wait for commit results
     * @param task the task which produced the records for which the offsets have been committed
     */
    protected void commitOffsets(OffsetStorageWriter offsetWriter, long commitTimeoutMs, SourceTask task) {
        long started = clock.currentTimeInMillis();
        long timeout = started + commitTimeoutMs;
        if (!offsetWriter.beginFlush()) return;
        Future<Void> flush = offsetWriter.doFlush(this::completedFlush);
        if (flush == null) return; // no offsets to commit ...

        // Wait until the offsets are flushed ...
        try {
            flush.get(Math.max(timeout - clock.currentTimeInMillis(), 0), TimeUnit.MILLISECONDS);
            // if we've gotten this far, the offsets have been committed so notify the task
            task.commit();
            recordsSinceLastCommit = 0;
            timeOfLastCommitMillis = clock.currentTimeInMillis();
        } catch (InterruptedException e) {
            logger.warn("Flush of {} offsets interrupted, cancelling", this);
            offsetWriter.cancelFlush();
        } catch (ExecutionException e) {
            logger.error("Flush of {} offsets threw an unexpected exception: ", this, e);
            offsetWriter.cancelFlush();
        } catch (TimeoutException e) {
            logger.error("Timed out waiting to flush {} offsets to storage", this);
            offsetWriter.cancelFlush();
        }
    }

    protected void completedFlush(Throwable error, Void result) {
        if (error != null) {
            logger.error("Failed to flush {} offsets to storage: ", this, error);
        } else {
            logger.trace("Finished flushing {} offsets to storage", this);
        }
    }

    /**
     * Stop the execution of this embedded connector. This method does not block until the connector is stopped; use
     * {@link #await(long, TimeUnit)} for this purpose.
     *
     * @return {@code true} if the connector was {@link #run() running} and will eventually stop, or {@code false} if it was not
     *         running when this method is called
     * @see #await(long, TimeUnit)
     */
    public boolean stop() {
        logger.debug("Stopping the embedded engine");
        // Signal that the run() method should stop ...
        Thread thread = this.runningThread.getAndSet(null);
        if (thread != null) {
            logger.debug("Interruping the embedded engine's thread " + thread + " (already interrupted: " + thread.isInterrupted() + ")");
            // Interrupt the thread in case it is blocked while polling the task for records ...
            thread.interrupt();
            return true;
        }
        return false;
    }

    /**
     * Wait for the connector to complete processing. If the processor is not running, this method returns immediately; however,
     * if the processor is {@link #stop() stopped} and restarted before this method is called, this method will return only
     * when it completes the second time.
     *
     * @param timeout the maximum amount of time to wait before returning
     * @param unit the unit of time; may not be null
     * @return {@code true} if the connector completed within the timeout (or was not running), or {@code false} if it is still
     *         running when the timeout occurred
     * @throws InterruptedException if this thread is interrupted while waiting for the completion of the connector
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    @Override
    public String toString() {
        return "EmbeddedConnector{id=" + config.getString(ENGINE_NAME) + '}';
    }

    public static class EmbeddedConfig extends WorkerConfig {
        private static final ConfigDef CONFIG;

        static {
            ConfigDef config = baseConfigDef();
            Field.group(config, "file", OFFSET_STORAGE_FILE_FILENAME);
            Field.group(config, "kafka", OFFSET_STORAGE_KAFKA_TOPIC);
            Field.group(config, "kafka", OFFSET_STORAGE_KAFKA_PARTITIONS);
            Field.group(config, "kafka", OFFSET_STORAGE_KAFKA_REPLICATION_FACTOR);
            CONFIG = config;
        }

        public EmbeddedConfig(Map<String, String> props) {
            super(CONFIG, props);
        }
    }
}
