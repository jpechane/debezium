/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;
import io.debezium.util.HexConverter;

/**
 * A utility class to map LogMiner content resultSet values.
 * This class gracefully logs errors, loosing an entry is not critical.
 * The loss will be logged
 */
public class RowMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(RowMapper.class);

    // operations
    public static final int INSERT = 1;
    public static final int DELETE = 2;
    public static final int UPDATE = 3;
    public static final int DDL = 5;
    public static final int COMMIT = 7;
    public static final int MISSING_SCN = 34;
    public static final int ROLLBACK = 36;

    private static final int SCN = 1;
    private static final int SQL_REDO = 2;
    private static final int OPERATION_CODE = 3;
    private static final int CHANGE_TIME = 4;
    private static final int TX_ID = 5;
    private static final int CSF = 6;
    private static final int TABLE_NAME = 7;
    private static final int SEG_OWNER = 8;
    private static final int OPERATION = 9;
    private static final int USERNAME = 10;
    private static final int ROW_ID = 11;
    private static final int ROLLBACK_FLAG = 12;
    // todo: add these for recording
    // private static final int SESSION_NUMBER = 10;
    // private static final int SERIAL_NUMBER = 11;
    // private static final int RS_ID = 12;
    // private static final int SSN = 12;

    public static String getOperation(OracleStreamingChangeEventSourceMetrics streamingMetrics, ResultSet rs) {
        try {
            return rs.getString(OPERATION);
        }
        catch (SQLException e) {
            logError(streamingMetrics, e, "OPERATION");
            return null;
        }
    }

    public static String getUsername(OracleStreamingChangeEventSourceMetrics streamingMetrics, ResultSet rs) {
        try {
            return rs.getString(USERNAME);
        }
        catch (SQLException e) {
            logError(streamingMetrics, e, "USERNAME");
            return null;
        }
    }

    public static int getOperationCode(OracleStreamingChangeEventSourceMetrics streamingMetrics, ResultSet rs) {
        try {
            return rs.getInt(OPERATION_CODE);
        }
        catch (SQLException e) {
            logError(streamingMetrics, e, "OPERATION_CODE");
            return 0;
        }
    }

    public static String getTableName(OracleStreamingChangeEventSourceMetrics streamingMetrics, ResultSet rs) {
        try {
            return rs.getString(TABLE_NAME);
        }
        catch (SQLException e) {
            logError(streamingMetrics, e, "TABLE_NAME");
            return "";
        }
    }

    public static String getSegOwner(OracleStreamingChangeEventSourceMetrics streamingMetrics, ResultSet rs) {
        try {
            return rs.getString(SEG_OWNER);
        }
        catch (SQLException e) {
            logError(streamingMetrics, e, "SEG_OWNER");
            return "";
        }
    }

    public static Timestamp getChangeTime(OracleStreamingChangeEventSourceMetrics streamingMetrics, ResultSet rs) {
        try {
            return rs.getTimestamp(CHANGE_TIME);
        }
        catch (SQLException e) {
            logError(streamingMetrics, e, "CHANGE_TIME");
            return new Timestamp(Instant.now().getEpochSecond());
        }
    }

    public static Scn getScn(OracleStreamingChangeEventSourceMetrics streamingMetrics, ResultSet rs) {
        try {
            return Scn.valueOf(rs.getString(SCN));
        }
        catch (SQLException e) {
            logError(streamingMetrics, e, "SCN");
            return Scn.NULL;
        }
    }

    public static String getTransactionId(OracleStreamingChangeEventSourceMetrics streamingMetrics, ResultSet rs) {
        try {
            return HexConverter.convertToHexString(rs.getBytes(TX_ID));
        }
        catch (SQLException e) {
            logError(streamingMetrics, e, "TX_ID");
            return "";
        }
    }

    /**
     * It constructs REDO_SQL. If REDO_SQL  is in a few lines, it truncates after first 40_000 characters
     * It also records LogMiner history info if isDml is true
     *
     * @param streaingMetrics the streaming metrics
     * @param rs result set
     * @param isDml flag indicating if operation code is a DML
     * @param historyRecorder history recorder
     * @param scn scn
     * @param tableName table name
     * @param segOwner segment owner
     * @param operationCode operation code
     * @param changeTime time of change
     * @param txId transaction ID
     * @return the redo SQL
     */
    public static String getSqlRedo(OracleStreamingChangeEventSourceMetrics streaingMetrics, ResultSet rs, boolean isDml,
                                    HistoryRecorder historyRecorder, Scn scn, String tableName,
                                    String segOwner, int operationCode, Timestamp changeTime, String txId) {
        int lobLimitCounter = 9; // todo : decide on approach ( XStream chunk option) and Lob limit
        StringBuilder result = new StringBuilder(4000);
        try {
            String redoSql = rs.getString(SQL_REDO);
            if (redoSql == null) {
                return null;
            }
            result = new StringBuilder(redoSql);

            int csf = rs.getInt(CSF);
            if (isDml) {
                historyRecorder.record(scn, tableName, segOwner, operationCode, changeTime, txId, csf, redoSql);
            }

            // 0 - indicates SQL_REDO is contained within the same row
            // 1 - indicates that either SQL_REDO is greater than 4000 bytes in size and is continued in
            // the next row returned by the ResultSet
            while (csf == 1) {
                rs.next();
                if (lobLimitCounter-- == 0) {
                    LOGGER.warn("LOB value was truncated due to the connector limitation of {} MB", 40);
                    break;
                }
                result.append(rs.getString(SQL_REDO));
                csf = rs.getInt(CSF);
                if (isDml) {
                    historyRecorder.record(scn, tableName, segOwner, operationCode, changeTime, txId, csf, rs.getString(SQL_REDO));
                }
            }

        }
        catch (SQLException e) {
            logError(streaingMetrics, e, "SQL_REDO");
        }
        return result.toString();
    }

    public static String getRowId(OracleStreamingChangeEventSourceMetrics streamingMetrics, ResultSet rs) {
        try {
            return rs.getString(ROW_ID);
        }
        catch (SQLException e) {
            logError(streamingMetrics, e, "ROW_ID");
            return null;
        }
    }

    public static int getRollbackFlag(OracleStreamingChangeEventSourceMetrics streamingMetrics, ResultSet rs) {
        try {
            return rs.getInt(ROLLBACK_FLAG);
        }
        catch (SQLException e) {
            logError(streamingMetrics, e, "ROLLBACK");
            return 0;
        }
    }

    private static void logError(OracleStreamingChangeEventSourceMetrics streamingMetrics, SQLException e, String s) {
        LogMinerHelper.logError(streamingMetrics, "Cannot get {}. This entry from LogMiner will be lost due to the {}", s, e);
    }

    public static TableId getTableId(String catalogName, ResultSet rs) {
        try {
            return new TableId(catalogName, rs.getString(SEG_OWNER), rs.getString(TABLE_NAME));
        }
        catch (SQLException e) {
            throw new DebeziumException("Cannot resolve TableId from result set data", e);
        }
    }

}
