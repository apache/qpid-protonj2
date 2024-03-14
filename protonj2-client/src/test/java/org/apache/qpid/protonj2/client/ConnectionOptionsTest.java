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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.junit.jupiter.api.Test;

public class ConnectionOptionsTest {

    @Test
    public void testCreate() {
        ConnectionOptions options = new ConnectionOptions();

        assertNull(options.password());
        assertNull(options.user());
        assertNull(options.virtualHost());
    }

    @Test
    public void testSetIdleTimeoutValidesUIntRange() {
        ConnectionOptions options = new ConnectionOptions();

        assertThrows(NumberFormatException.class, () -> options.idleTimeout(UnsignedInteger.MAX_VALUE.longValue() + 1));
    }

    @Test
    public void testCreateDefaultsTimeouts() {
        ConnectionOptions options = new ConnectionOptions();

        assertEquals(ConnectionOptions.DEFAULT_CHANNEL_MAX, options.channelMax());
        assertEquals(ConnectionOptions.DEFAULT_MAX_FRAME_SIZE, options.maxFrameSize());
        assertEquals(ConnectionOptions.DEFAULT_OPEN_TIMEOUT, options.openTimeout());
        assertEquals(ConnectionOptions.DEFAULT_CLOSE_TIMEOUT, options.closeTimeout());
        assertEquals(ConnectionOptions.DEFAULT_SEND_TIMEOUT, options.sendTimeout());
        assertEquals(ConnectionOptions.DEFAULT_REQUEST_TIMEOUT, options.requestTimeout());
        assertEquals(ConnectionOptions.DEFAULT_IDLE_TIMEOUT, options.idleTimeout());
        assertEquals(ConnectionOptions.DEFAULT_DRAIN_TIMEOUT, options.drainTimeout());
    }

    @Test
    public void testCopy() {
        final ConnectionOptions options = new ConnectionOptions();
        final String[] offeredCapabilities = new String[] { "one", "two", "three" };
        final String[] desiredCapabilities = new String[] { "four", "five", "six" };
        final Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("one", "1");
        properties.put("two", "2");

        options.user("test");
        options.password("test-pass");
        options.virtualHost("test");
        options.openTimeout(10);
        options.closeTimeout(20);
        options.sendTimeout(30);
        options.requestTimeout(40);
        options.idleTimeout(50);
        options.drainTimeout(60);
        options.channelMax(1);
        options.maxFrameSize(1024);
        options.traceFrames(true);
        options.defaultNextReceiverPolicy(NextReceiverPolicy.FIRST_AVAILABLE);
        options.offeredCapabilities(offeredCapabilities);
        options.desiredCapabilities(desiredCapabilities);
        options.properties(properties);
        options.transportOptions().allowNativeIO(false);
        options.transportOptions().defaultTcpPort(8086);
        options.sslOptions().allowNativeSSL(false);
        options.sslOptions().defaultSslPort(8087);
        options.saslOptions().saslEnabled(false);

        ConnectionOptions copy = options.clone();

        assertNotSame(copy, options);
        assertNotSame(copy.saslOptions(), options.saslOptions());
        assertNotSame(copy.transportOptions(), options.transportOptions());
        assertNotSame(copy.sslOptions(), options.sslOptions());
        assertNotSame(copy.offeredCapabilities(), options.offeredCapabilities());
        assertNotSame(copy.desiredCapabilities(), options.desiredCapabilities());
        assertNotSame(copy.properties(), options.properties());

        assertArrayEquals(copy.offeredCapabilities(), offeredCapabilities);
        assertArrayEquals(copy.desiredCapabilities(), desiredCapabilities);

        assertEquals(options.properties(), properties);
        assertEquals(options.user(), copy.user());
        assertEquals(options.password(), copy.password());
        assertEquals(options.virtualHost(), copy.virtualHost());
        assertEquals(options.openTimeout(), copy.openTimeout());
        assertEquals(options.closeTimeout(), copy.closeTimeout());
        assertEquals(options.sendTimeout(), copy.sendTimeout());
        assertEquals(options.requestTimeout(), copy.requestTimeout());
        assertEquals(options.idleTimeout(), copy.idleTimeout());
        assertEquals(options.drainTimeout(), copy.drainTimeout());
        assertEquals(options.channelMax(), copy.channelMax());
        assertEquals(options.maxFrameSize(), copy.maxFrameSize());
        assertEquals(options.traceFrames(), copy.traceFrames());
        assertEquals(options.defaultNextReceiverPolicy(), copy.defaultNextReceiverPolicy());
        assertEquals(options.saslOptions().saslEnabled(), copy.saslOptions().saslEnabled());
        assertEquals(options.saslOptions().saslEnabled(), copy.saslOptions().saslEnabled());
        assertEquals(options.transportOptions().defaultTcpPort(), copy.transportOptions().defaultTcpPort());
        assertEquals(options.transportOptions().allowNativeIO(), copy.transportOptions().allowNativeIO());
        assertEquals(options.sslOptions().defaultSslPort(), copy.sslOptions().defaultSslPort());
        assertEquals(options.sslOptions().allowNativeSSL(), copy.sslOptions().allowNativeSSL());
        assertEquals(options.saslOptions().saslEnabled(), copy.saslOptions().saslEnabled());
    }

    @Test
    public void testCopySaslOptions() {
        ConnectionOptions options = new ConnectionOptions();

        options.saslOptions().addAllowedMechanism("PLAIN");
        options.saslOptions().addAllowedMechanism("ANONYMOUS");
        options.saslOptions().saslEnabled(false);

        ConnectionOptions copy = options.clone();

        assertNotSame(copy, options);
        assertNotSame(copy.saslOptions(), options.saslOptions());

        assertEquals(options.saslOptions().allowedMechanisms(), copy.saslOptions().allowedMechanisms());
        assertEquals(options.saslOptions().saslEnabled(), copy.saslOptions().saslEnabled());
    }
}
