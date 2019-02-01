/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.time.Duration;

import org.apache.kafka.common.errors.TimeoutException;

/**
 * A calss that represents waiting for a condition to be fullfilled.
 *
 * @author Jiri Pechanec
 *
 */
public class Wait {

    @FunctionalInterface
    public static interface WaitCondition {
        boolean isCompleted();
    };

    public static Duration DEFAULT_PAUSE = Duration.ofMillis(100);
    public static int DEFAULT_RETRIES = 50;

    public static void condition(WaitCondition condition, Duration pauseBetweenRetries, int retries) throws InterruptedException {
        final Metronome m = Metronome.sleeper(pauseBetweenRetries, Clock.system());
        for (int i = 0; i < retries; i++) {
            if (condition.isCompleted()) {
                return;
            }
            m.pause();
        }
        throw new TimeoutException("Condiiton was not completed in requested time");
    }

    public static void condition(WaitCondition condition) throws InterruptedException {
        condition(condition, DEFAULT_PAUSE, DEFAULT_RETRIES);
    }
}
