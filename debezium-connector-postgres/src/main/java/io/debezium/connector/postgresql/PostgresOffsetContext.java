/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.time.Conversions;

public class PostgresOffsetContext implements OffsetContext {

    private static final String SERVER_PARTITION_KEY = "server";
    public static final String LAST_SNAPSHOT_RECORD_KEY = "last_snapshot_record";

    private final Schema sourceInfoSchema;
    private final SourceInfo sourceInfo;
    private final Map<String, String> partition;
    private boolean lastSnapshotRecord;

    public PostgresOffsetContext(String serverName, String databaseName, Long lsn, Long lastCompletelyProcessedLsn, Long txId, Instant time, boolean snapshot, boolean lastSnapshotRecord) {
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, serverName);
        sourceInfo = new SourceInfo(serverName, databaseName);

        sourceInfo.update(lsn, lastCompletelyProcessedLsn, time, txId, null, sourceInfo.xmin());
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
        if (sourceInfo.getLastCompletelyProcessedLsn() != null) {
            result.put(SourceInfo.LAST_COMPLETELY_PROCESSED_LSN_KEY, sourceInfo.getLastCompletelyProcessedLsn());
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

    public void updateWalPosition(Long lsn, Long lastCompletelyProcessedLsn, Instant commitTime, Long txId, TableId tableId, Long xmin) {
        sourceInfo.update(lsn, lastCompletelyProcessedLsn, commitTime, txId, tableId, xmin);
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

        private Long readOptionalLong(Map<String, ?> offset, String key) {
            final Object obj = offset.get(key);
            return (obj == null) ? null : ((Number) obj).longValue();
        }

        @SuppressWarnings("unchecked")
        @Override
        public OffsetContext load(Map<String, ?> offset) {
            final Long lsn = readOptionalLong(offset, SourceInfo.LSN_KEY);
            final Long lastCompletelyProcessedLsn = readOptionalLong(offset, SourceInfo.LAST_COMPLETELY_PROCESSED_LSN_KEY);
            final Long txId = readOptionalLong(offset, SourceInfo.TXID_KEY);

            final Instant useconds = Conversions.toInstantFromMicros((Long)offset.get(SourceInfo.TIMESTAMP_KEY));
            final boolean snapshot = (boolean)((Map<String, Object>)offset).getOrDefault(SourceInfo.SNAPSHOT_KEY, Boolean.FALSE);
            final boolean lastSnapshotRecord = (boolean)((Map<String, Object>)offset).getOrDefault(SourceInfo.LAST_SNAPSHOT_RECORD_KEY, Boolean.FALSE);
            return new PostgresOffsetContext(logicalName, databaseName, lsn, lastCompletelyProcessedLsn, txId, useconds, snapshot, lastSnapshotRecord); 
        }
    }

    @Override
    public String toString() {
        return "PostgresOffsetContext [sourceInfo=" + sourceInfo
                + ", partition=" + partition
                + ", lastSnapshotRecord=" + lastSnapshotRecord + "]";
    }

    public SourceInfo getSI() {
        return sourceInfo;
    }
}
