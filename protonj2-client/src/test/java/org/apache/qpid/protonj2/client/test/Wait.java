/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.protonj2.client.test;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.TimeUnit;

public class Wait {

    public static final long MAX_WAIT_MILLIS = 30 * 1000;
    public static final long SLEEP_MILLIS = 200;
    public static final String DEFAULT_FAILURE_MESSAGE = "Expected condition was not met";

    @FunctionalInterface
    public interface Condition {
        boolean isSatisfied() throws Exception;
    }

    public static void assertTrue(Condition condition) {
        assertTrue(DEFAULT_FAILURE_MESSAGE, condition);
    }

    public static void assertFalse(Condition condition) throws Exception {
        assertTrue(() -> !condition.isSatisfied());
    }

    public static void assertFalse(String failureMessage, Condition condition) {
        assertTrue(failureMessage, () -> !condition.isSatisfied());
    }

    public static void assertFalse(String failureMessage, Condition condition, final long duration) {
        assertTrue(failureMessage, () -> !condition.isSatisfied(), duration, SLEEP_MILLIS);
    }

    public static void assertFalse(Condition condition, final long duration, final long sleep) {
        assertTrue(DEFAULT_FAILURE_MESSAGE, () -> !condition.isSatisfied(), duration, sleep);
    }

    public static void assertTrue(Condition condition, final long duration) {
        assertTrue(DEFAULT_FAILURE_MESSAGE, condition, duration, SLEEP_MILLIS);
    }

    public static void assertTrue(String failureMessage, Condition condition) {
        assertTrue(failureMessage, condition, MAX_WAIT_MILLIS);
    }

    public static void assertTrue(String failureMessage, Condition condition, final long duration) {
        assertTrue(failureMessage, condition, duration, SLEEP_MILLIS);
    }

    public static void assertTrue(Condition condition, final long duration, final long sleep) throws Exception {
        assertTrue(DEFAULT_FAILURE_MESSAGE, condition, duration, sleep);
    }

    public static void assertTrue(String failureMessage, Condition condition, final long duration, final long sleep) {
        boolean result = waitFor(condition, duration, sleep);

        if (!result) {
            fail(failureMessage);
        }
    }

    public static boolean waitFor(Condition condition) throws Exception {
        return waitFor(condition, MAX_WAIT_MILLIS);
    }

    public static boolean waitFor(final Condition condition, final long duration) throws Exception {
        return waitFor(condition, duration, SLEEP_MILLIS);
    }

    public static boolean waitFor(final Condition condition, final long durationMillis, final long sleepMillis) {
        try {
            final long expiry = System.currentTimeMillis() + durationMillis;
            boolean conditionSatisfied = condition.isSatisfied();

            while (!conditionSatisfied && System.currentTimeMillis() < expiry) {
                if (sleepMillis == 0) {
                    Thread.yield();
                } else {
                    TimeUnit.MILLISECONDS.sleep(sleepMillis);
                }
                conditionSatisfied = condition.isSatisfied();
            }

            return conditionSatisfied;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
