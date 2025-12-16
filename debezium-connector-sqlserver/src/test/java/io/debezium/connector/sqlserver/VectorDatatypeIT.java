/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration test to verify support for VECTOR datatype in SQL Server.
 *
 * @author Jiri Pechanec
 */
public class VectorDatatypeIT extends AbstractAsyncEngineConnectorTest {

    private SqlServerConnection connection;

    @AfterEach
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("dbz#1208")
    public void doubleVector() throws Exception {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        final Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        final var c = connection.connection();
        c.setAutoCommit(true);
        c.createStatement().execute("ALTER DATABASE SCOPED CONFIGURATION SET PREVIEW_FEATURES = ON");
        c.setAutoCommit(false);

        connection.execute(
            "CREATE TABLE vector_table (id int primary key, cola VECTOR(3) NOT NULL)",
            "INSERT INTO vector_table (id, cola) VALUES (1, '[0.1, 2, 30]')"
        );

        TestHelper.enableTableCdc(connection, "vector_table");
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();

        final var actualRecords = consumeRecordsByTopic(1, false);
        var vectorRecords = actualRecords.recordsForTopic("server1.testDB1.dbo.vector_table");
        assertEquals(1, vectorRecords.size());
        final var rec1 = vectorRecords.get(0);
        System.out.println(rec1);
    }
}