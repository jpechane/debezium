package io.debezium.connector.mysql.cube;

import io.debezium.config.Configuration.Builder;

public interface DatabaseCube {

    public String getCubeName();
    public Builder configuration();
    public String getCubeIP();
}
