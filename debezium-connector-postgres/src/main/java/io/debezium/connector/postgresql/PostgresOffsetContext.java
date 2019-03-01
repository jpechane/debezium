/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;

public class PostgresOffsetContext implements OffsetContext {

    private static final String SERVER_PARTITION_KEY = "server";
    public static final String LAST_SNAPSHOT_RECORD_KEY = "last_snapshot_record";

    private final Schema sourceInfoSchema;
    private final SourceInfo sourceInfo;
    private final Map<String, String> partition;
    private boolean lastSnapshotRecord;

    public PostgresOffsetContext(String serverName, String databaseName, Long lsn, Long txId, Long time, boolean snapshot, boolean lastSnapshotRecord) {
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, serverName);
        sourceInfo = new SourceInfo(serverName, databaseName);

        sourceInfo.update(lsn, time, txId, null);
        sourceInfoSchema = sourceInfo.schema();

        this.lastSnapshotRecord = lastSnapshotRecord;
        if (this.lastSnapshotRecord) {
            postSnapshotCompletion();
        }
        else if (snapshot) {
            sourceInfo.startSnapshot();
        }
    }

    @Override
    public Map<String, ?> getPartition() {
        return partition;
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> result = new HashMap<>();
        if (sourceInfo.getUseconds() != null) {
            result.put(SourceInfo.TIMESTAMP_KEY, sourceInfo.getUseconds());
        }
        if (sourceInfo.getTxId() != null) {
            result.put(SourceInfo.TXID_KEY, sourceInfo.getTxId());
        }
        if (sourceInfo.getLsn() != null) {
            result.put(SourceInfo.LSN_KEY, sourceInfo.getLsn());
        }
        if (isSnapshotRunning() || lastSnapshotRecord) {
            result.put(SourceInfo.SNAPSHOT_KEY, true);
            result.put(SourceInfo.LAST_SNAPSHOT_RECORD_KEY, sourceInfo.isLastSnapshotRecord());
        }
        return result;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshotInEffect();
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.startSnapshot();
    }

    @Override
    public void preSnapshotCompletion() {
        lastSnapshotRecord = true;
        sourceInfo.markLastSnapshotRecord();
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.completeSnapshot();
        lastSnapshotRecord = false;
    }

    public void updateSnapshotPosition(long time, TableId tableId) {
        sourceInfo.update(time, tableId);
    }

    public static class Loader implements OffsetContext.Loader {

        private final String logicalName;
        private final String databaseName;

        public Loader(String logicalName, String databaseName) {
            this.logicalName = logicalName;
            this.databaseName = databaseName;
        }

        @Override
        public Map<String, ?> getPartition() {
            return Collections.singletonMap(SERVER_PARTITION_KEY, logicalName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public OffsetContext load(Map<String, ?> offset) {
            final long lsn = ((Number)offset.get(SourceInfo.LSN_KEY)).longValue();
            final long txId = ((Number)offset.get(SourceInfo.TXID_KEY)).longValue();
            final long useconds = (Long)offset.get(SourceInfo.TIMESTAMP_KEY);
            final boolean snapshot = (boolean)((Map<String, Object>)offset).getOrDefault(SourceInfo.SNAPSHOT_KEY, Boolean.FALSE);
            final boolean lastSnapshotRecord = (boolean)((Map<String, Object>)offset).getOrDefault(SourceInfo.LAST_SNAPSHOT_RECORD_KEY, Boolean.FALSE);
            return new PostgresOffsetContext(logicalName, databaseName, lsn, txId, useconds, snapshot, lastSnapshotRecord); 
        }
    }

    @Override
    public String toString() {
        return "PostgresOffsetContext [sourceInfo=" + sourceInfo
                + ", partition=" + partition
                + ", lastSnapshotRecord=" + lastSnapshotRecord + "]";
    }
}
