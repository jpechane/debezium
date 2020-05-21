/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.spi;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 * Contract for differnt writers responsible for persistent storage of serialized
 * offsets.
 * 
 * @author Jiri Pechanec
 *
 */
public interface OffsetWriter extends Closeable {

    /**
     * Use the specified configuration properties for the writer.
     *
     * @param config the configuration
     */
    void configure(Properties config);

    /**
     * Start the writer.
     */
    void start();

    /**
     * Get the offsets for the specific keys.
     * 
     * @param keys list of keys to look up
     * @return the map of offset values
     */
    Map<String, byte[]> read(Collection<String> keys);

    /**
     * Write offsets into the store.
     * 
     * @param offsets offsets to be stored
     */
    void write(Map<String, byte[]> offsets);
}

