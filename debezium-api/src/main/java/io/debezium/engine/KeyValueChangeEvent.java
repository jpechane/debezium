/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine;

public interface KeyValueChangeEvent<K, V> {

    public K key();

    public V value();
}
