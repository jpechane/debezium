/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import org.arquillian.cube.docker.impl.await.LogScanningAwaitStrategy;
import org.arquillian.cube.docker.impl.await.PollingAwaitStrategy;
import org.arquillian.cube.docker.impl.client.config.Await;
import org.arquillian.cube.docker.impl.docker.DockerClientExecutor;
import org.arquillian.cube.spi.Cube;
import org.arquillian.cube.spi.await.AwaitStrategy;

public class MySQLAwaitStrategy implements AwaitStrategy {
    private static final int WAIT_AFTER_LOG_MESSAGE = 5000;
    private Await params;
    private DockerClientExecutor dockerClientExecutor;
    private Cube<?> cube;

    public void setCube(Cube<?> cube) {
        this.cube = cube;
    }

    public void setDockerClientExecutor(DockerClientExecutor dockerClientExecutor) {
        this.dockerClientExecutor = dockerClientExecutor;
    }

    public void setParams(Await params) {
        this.params = params;
    }

    @Override
    public boolean await() {
        final LogScanningAwaitStrategy log = new LogScanningAwaitStrategy(cube, dockerClientExecutor, params);
        final PollingAwaitStrategy polling = new PollingAwaitStrategy(cube, dockerClientExecutor, params);

        final boolean logAwait = log.await();
        if (logAwait) {
            try {
                Thread.sleep(WAIT_AFTER_LOG_MESSAGE);
            } catch (InterruptedException e) {
            }
        }
        else {
            return false;
        }

        return polling.await();
    }}
