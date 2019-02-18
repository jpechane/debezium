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
import io.debezium.util.Collect;

public class PostgresOffsetContext implements OffsetContext {

    private static final String SERVER_PARTITION_KEY = "server";
    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    private final Schema sourceInfoSchema;
    private final SourceInfo sourceInfo;
    private final Map<String, String> partition;
    private boolean snapshotCompleted;

    public PostgresOffsetContext(String serverName, TxLogPosition position, boolean snapshot, boolean snapshotCompleted) {
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, serverName);
        sourceInfo = new SourceInfo(serverName);

        sourceInfo.setCommitLsn(position.getCommitLsn());
        sourceInfo.setChangeLsn(position.getInTxLsn());
        sourceInfoSchema = sourceInfo.schema();

        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot);
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
        if (!snapshotCompleted) {
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

    public TxLogPosition getChangePosition() {
        return TxLogPosition.valueOf(sourceInfo.getCommitLsn(), sourceInfo.getChangeLsn());
    }

    public void setChangePosition(TxLogPosition position) {
        sourceInfo.setCommitLsn(position.getCommitLsn());
        sourceInfo.setChangeLsn(position.getInTxLsn());
    }

    public void setSourceTime(Instant instant) {
        sourceInfo.setSourceTime(instant);
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(true);
        snapshotCompleted = false;
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(false);
    }

    public static class Loader implements OffsetContext.Loader {

        private final String logicalName;

        public Loader(String logicalName) {
            this.logicalName = logicalName;
        }

        @Override
        public Map<String, ?> getPartition() {
            return Collections.singletonMap(SERVER_PARTITION_KEY, logicalName);
        }

        @Override
        public OffsetContext load(Map<String, ?> offset) {
            final Lsn changeLsn = Lsn.valueOf((String)offset.get(SourceInfo.CHANGE_LSN_KEY));
            final Lsn commitLsn = Lsn.valueOf((String)offset.get(SourceInfo.COMMIT_LSN_KEY));
            boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
            boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY));

            return new PostgresOffsetContext(logicalName, TxLogPosition.valueOf(commitLsn, changeLsn), snapshot, snapshotCompleted);
        }
    }

    @Override
    public String toString() {
        return "SqlServerOffsetContext [" +
                "sourceInfoSchema=" + sourceInfoSchema +
                ", sourceInfo=" + sourceInfo +
                ", partition=" + partition +
                ", snapshotCompleted=" + snapshotCompleted +
                "]";
    }
}
