/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import com.github.dockerjava.api.DockerClient;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;

enum MySQLCube {
    DEFAULT("mysql-server"),
    GTIDS_MASTER("database-gtids"),
    GTIDS_REPLICA("replica-gtids");

    public static final int MYSQL_PORT = 3306;

    private final String cubeName;

    private MySQLCube(final String cubeName) {
        this.cubeName = cubeName;
    }

    public String getCubeName() {
        return cubeName;
    }

    public Builder configuration(final DockerClient docker) {
        return Configuration.create()
                .with(MySqlConnectorConfig.HOSTNAME, getCubeIP(docker))
                .with(MySqlConnectorConfig.PORT, MYSQL_PORT);
    }

    @SuppressWarnings("deprecation")
    public String getCubeIP(final DockerClient docker) {
        return docker.inspectContainerCmd(getCubeName()).exec().getNetworkSettings().getIpAddress();
    }
}