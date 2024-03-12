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
package org.apache.qpid.protonj2.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.junit.jupiter.api.Test;

public class RecconnectOptionsTest {

    @Test
    public void testCreate() {
        ReconnectOptions options = new ReconnectOptions();

        assertEquals(options.reconnectEnabled(), ReconnectOptions.DEFAULT_RECONNECT_ENABLED);
        assertEquals(options.warnAfterReconnectAttempts(), ReconnectOptions.DEFAULT_WARN_AFTER_RECONNECT_ATTEMPTS);
        assertEquals(options.maxInitialConnectionAttempts(), ReconnectOptions.INFINITE);
        assertEquals(options.maxReconnectAttempts(), ReconnectOptions.INFINITE);
        assertEquals(options.reconnectBackOffMultiplier(), ReconnectOptions.DEFAULT_RECONNECT_BACKOFF_MULTIPLIER);
        assertEquals(options.useReconnectBackOff(), ReconnectOptions.DEFAULT_USE_RECONNECT_BACKOFF);
        assertEquals(options.maxReconnectDelay(), ReconnectOptions.DEFAULT_MAX_RECONNECT_DELAY);
        assertEquals(options.reconnectDelay(), ReconnectOptions.DEFAULT_RECONNECT_DELAY);
    }

    @Test
    public void testCopy() {
        ReconnectOptions options = new ReconnectOptions();

        options.useReconnectBackOff(true);
        options.maxReconnectAttempts(50);
        options.maxReconnectDelay(15);

        ReconnectOptions copy = options.clone();

        assertNotSame(copy, options);
        assertFalse(options.reconnectEnabled());
        assertEquals(options.useReconnectBackOff(), true);
        assertEquals(options.maxReconnectAttempts(), 50);
        assertEquals(options.maxReconnectDelay(), 15);
    }
}
