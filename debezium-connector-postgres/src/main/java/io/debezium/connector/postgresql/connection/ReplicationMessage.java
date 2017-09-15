/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.util.List;

import io.debezium.connector.postgresql.RecordsStreamProducer.PgConnectionSupplier;

/**
 * An abstract represantion of replication message that is sent by PostgreSQL logical decoding plugin and
 * is processed by Debezium PostgreSQL connector.
 * 
 * @author Jiri Pechanec
 *
 */
public interface ReplicationMessage {

    /**
     * 
     * Data modification operation executed
     *
     */
    public enum Operation {
        INSERT, UPDATE, DELETE
    }

    /**
     * 
     * A representation of column value delivered as a part of replication message
     *
     */
    public abstract class Column {
        private final String name;
        private final long type;

        public Column(final String name, final long type) {
            super();
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public long getType() {
            return type;
        }

        public abstract Object getValue(final PgConnectionSupplier connection);
    }

    /**
     * @return A data operation executed
     */
    public Operation getOperation();

    /**
     * @return Transaction commit time for this change
     */
    public long getCommitTime();

    /**
     * @return An id of transaction to which this change belongs
     */
    public int getTransactionId();

    /**
     * @return Table changed
     */
    public String getTable();

    /**
     * @return Set of original values of table columns, null for INSERT
     */
    public List<Column> getOldTupleList();

    /**
     * @return Set of new values of table columns, null for DELETE
     */
    public List<Column> getNewTupleList();

}
