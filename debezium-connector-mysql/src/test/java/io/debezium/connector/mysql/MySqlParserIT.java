/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.fest.assertions.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.SQLException;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Testing;

@SkipWhenDatabaseVersion(check = LESS_THAN, major = 8, reason = "DDL uses column constraints, not supported until MySQL 8.0")
public class MySqlParserIT extends AbstractConnectorTest {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-column-constraint.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("myServer1", "connector_test")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    public void parseTableWithVisibleColumns() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("CREATE TABLE VISIBLE_COLUMN_TABLE (" +
                        "    ID BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                        "    NAME VARCHAR(100) NOT NULL," +
                        "    WORK_ID BIGINT VISIBLE" +
                        ");");
                connection.execute("INSERT INTO VISIBLE_COLUMN_TABLE VALUES (1001,'Larry',113);");
                connection.query("SELECT * FROM VISIBLE_COLUMN_TABLE", rs -> {
                    if (Testing.Print.isEnabled()) {
                        connection.print(rs);
                    }
                });
            }
        }
        SourceRecords records = consumeRecordsByTopic(2);

        final SourceRecord recordStream = records.recordsForTopic(DATABASE.topicForTable("VISIBLE_COLUMN_TABLE")).get(0);
        assertThat(((Struct) recordStream.value()).getStruct("after").getString("WORK_ID")).isEqualTo("113");

        assertThat(records.recordsForTopic(DATABASE.topicForTable("VISIBLE_COLUMN_TABLE")).size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(1);
    }

    @Test
    public void parseTableWithInVisibleColumns() throws SQLException, InterruptedException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.INITIAL)
                .build();

        // Start the connector ...
        start(MySqlConnector.class, config);

        Testing.Print.enable();

        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName())) {
            try (JdbcConnection connection = db.connect()) {
                connection.execute("CREATE TABLE INVISIBLE_COLUMN_TABLE (" +
                        "    ID BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                        "    NAME VARCHAR(100) NOT NULL," +
                        "    WORK_ID BIGINT INVISIBLE" +
                        ");");
                connection.execute("INSERT INTO INVISIBLE_COLUMN_TABLE VALUES (1002,'Jack',111);");
                connection.query("SELECT * FROM INVISIBLE_COLUMN_TABLE", rs -> {
                    if (Testing.Print.isEnabled()) {
                        connection.print(rs);
                    }
                });
            }
        }
        SourceRecords records = consumeRecordsByTopic(2);

        final SourceRecord recordStream = records.recordsForTopic(DATABASE.topicForTable("INVISIBLE_COLUMN_TABLE")).get(0);
        assertThat(((Struct) recordStream.value()).getStruct("after").getString("WORK_ID")).isEqualTo("111");

        assertThat(records.recordsForTopic(DATABASE.topicForTable("INVISIBLE_COLUMN_TABLE")).size()).isEqualTo(1);
        assertThat(records.ddlRecordsForDatabase(DATABASE.getDatabaseName()).size()).isEqualTo(1);
    }
}
