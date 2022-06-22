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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;

import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.junit.jupiter.api.Test;

/**
 * Test for class TransportOptions
 */
public class TransportOptionsTest extends ImperativeClientTestCase {

    public static final int TEST_SEND_BUFFER_SIZE = 128 * 1024;
    public static final int TEST_RECEIVE_BUFFER_SIZE = TEST_SEND_BUFFER_SIZE;
    public static final int TEST_TRAFFIC_CLASS = 1;
    public static final boolean TEST_TCP_NO_DELAY = false;
    public static final boolean TEST_TCP_KEEP_ALIVE = true;
    public static final int TEST_SO_LINGER = Short.MAX_VALUE;
    public static final int TEST_SO_TIMEOUT = 10;
    public static final int TEST_CONNECT_TIMEOUT = 90000;
    public static final int TEST_DEFAULT_TCP_PORT = 5682;
    public static final String LOCAL_ADDRESS = "localhost";
    public static final int LOCAL_PORT = 30000;
    public static final boolean TEST_ALLOW_NATIVE_IO_VALUE = !TransportOptions.DEFAULT_ALLOW_NATIVE_IO;
    public static final boolean TEST_TRACE_BYTES_VALUE = !TransportOptions.DEFAULT_TRACE_BYTES;
    public static final String TEST_WEBSOCKET_PATH = "/test";
    public static final String TEST_WEBSOCKET_HEADER_KEY = "compression";
    public static final String TEST_WEBSOCKET_HEADER_VALUE = "gzip";
    public static final int TEST_WEBSOCKET_MAX_FRAME_SIZE = TransportOptions.DEFAULT_WEBSOCKET_MAX_FRAME_SIZE + 1024;

    @Test
    public void testCreate() {
        TransportOptions options = new TransportOptions();

        assertEquals(TransportOptions.DEFAULT_TCP_NO_DELAY, options.tcpNoDelay());

        assertTrue(options.allowNativeIO());
        assertFalse(options.useWebSockets());
        assertNull(options.webSocketPath());
    }

    @Test
    public void testOptions() {
        TransportOptions options = createNonDefaultOptions();

        assertEquals(TEST_SEND_BUFFER_SIZE, options.sendBufferSize());
        assertEquals(TEST_RECEIVE_BUFFER_SIZE, options.receiveBufferSize());
        assertEquals(TEST_TRAFFIC_CLASS, options.trafficClass());
        assertEquals(TEST_TCP_NO_DELAY, options.tcpNoDelay());
        assertEquals(TEST_TCP_KEEP_ALIVE, options.tcpKeepAlive());
        assertEquals(TEST_SO_LINGER, options.soLinger());
        assertEquals(TEST_SO_TIMEOUT, options.soTimeout());
        assertEquals(TEST_CONNECT_TIMEOUT, options.connectTimeout());
        assertEquals(TEST_DEFAULT_TCP_PORT, options.defaultTcpPort());
        assertEquals(TEST_ALLOW_NATIVE_IO_VALUE, options.allowNativeIO());
        assertEquals(TEST_TRACE_BYTES_VALUE, options.traceBytes());
    }

    @Test
    public void testClone() {
        TransportOptions original = createNonDefaultOptions();
        TransportOptions options = original.clone();

        assertNotSame(original, options);

        assertEquals(TEST_SEND_BUFFER_SIZE, options.sendBufferSize());
        assertEquals(TEST_RECEIVE_BUFFER_SIZE, options.receiveBufferSize());
        assertEquals(TEST_TRAFFIC_CLASS, options.trafficClass());
        assertEquals(TEST_TCP_NO_DELAY, options.tcpNoDelay());
        assertEquals(TEST_TCP_KEEP_ALIVE, options.tcpKeepAlive());
        assertEquals(TEST_SO_LINGER, options.soLinger());
        assertEquals(TEST_SO_TIMEOUT, options.soTimeout());
        assertEquals(TEST_CONNECT_TIMEOUT, options.connectTimeout());
        assertEquals(TEST_DEFAULT_TCP_PORT, options.defaultTcpPort());
        assertEquals(TEST_ALLOW_NATIVE_IO_VALUE, options.allowNativeIO());
        assertEquals(TEST_TRACE_BYTES_VALUE, options.traceBytes());
        assertEquals(LOCAL_ADDRESS,options.localAddress());
        assertEquals(LOCAL_PORT,options.localPort());
        assertEquals(TEST_WEBSOCKET_PATH, options.webSocketPath());
        assertEquals(TEST_WEBSOCKET_HEADER_VALUE, options.webSocketHeaders().get(TEST_WEBSOCKET_HEADER_KEY));
        assertEquals(TEST_WEBSOCKET_MAX_FRAME_SIZE, options.webSocketMaxFrameSize());
    }

    @Test
    public void testSendBufferSizeValidation() {
        TransportOptions options = createNonDefaultOptions().clone();
        try {
            options.sendBufferSize(0);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
        try {
            options.sendBufferSize(-1);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }

        options.sendBufferSize(1);
    }

    @Test
    public void testReceiveBufferSizeValidation() {
        TransportOptions options = createNonDefaultOptions().clone();
        try {
            options.receiveBufferSize(0);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
        try {
            options.receiveBufferSize(-1);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }

        options.receiveBufferSize(1);
    }

    @Test
    public void testTrafficClassValidation() {
        TransportOptions options = createNonDefaultOptions().clone();
        try {
            options.trafficClass(-1);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
        try {
            options.trafficClass(256);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }

        options.trafficClass(0);
        options.trafficClass(128);
        options.trafficClass(255);
    }

    @Test
    public void testNativeIOPreferencesCannotBeNulled() {
        TransportOptions options = createNonDefaultOptions();

        assertNotNull(options.nativeIOPreference());
        assertEquals(TransportOptions.DEFAULT_NATIVEIO_PREFERENCES, Arrays.asList(options.nativeIOPreference()));

        options.nativeIOPreference((String) null);

        assertNotNull(options.nativeIOPreference());
        assertEquals(TransportOptions.DEFAULT_NATIVEIO_PREFERENCES, Arrays.asList(options.nativeIOPreference()));

        options.nativeIOPreference("epolling");

        assertNotNull(options.nativeIOPreference());
        assertArrayEquals(new String[] { "epolling" }, options.nativeIOPreference());
    }

    @Test
    public void testCreateAndConfigure() {
        TransportOptions options = createNonDefaultOptions();

        assertEquals(TEST_SEND_BUFFER_SIZE, options.sendBufferSize());
        assertEquals(TEST_RECEIVE_BUFFER_SIZE, options.receiveBufferSize());
        assertEquals(TEST_TRAFFIC_CLASS, options.trafficClass());
        assertEquals(TEST_TCP_NO_DELAY, options.tcpNoDelay());
        assertEquals(TEST_TCP_KEEP_ALIVE, options.tcpKeepAlive());
        assertEquals(TEST_SO_LINGER, options.soLinger());
        assertEquals(TEST_SO_TIMEOUT, options.soTimeout());
        assertEquals(TEST_CONNECT_TIMEOUT, options.connectTimeout());
    }

    private TransportOptions createNonDefaultOptions() {
        TransportOptions options = new TransportOptions();

        options.sendBufferSize(TEST_SEND_BUFFER_SIZE);
        options.receiveBufferSize(TEST_RECEIVE_BUFFER_SIZE);
        options.trafficClass(TEST_TRAFFIC_CLASS);
        options.tcpNoDelay(TEST_TCP_NO_DELAY);
        options.tcpKeepAlive(TEST_TCP_KEEP_ALIVE);
        options.soLinger(TEST_SO_LINGER);
        options.soTimeout(TEST_SO_TIMEOUT);
        options.connectTimeout(TEST_CONNECT_TIMEOUT);
        options.defaultTcpPort(TEST_DEFAULT_TCP_PORT);
        options.allowNativeIO(TEST_ALLOW_NATIVE_IO_VALUE);
        options.traceBytes(TEST_TRACE_BYTES_VALUE);
        options.localAddress(LOCAL_ADDRESS);
        options.localPort(LOCAL_PORT);
        options.webSocketPath(TEST_WEBSOCKET_PATH);
        options.addWebSocketHeader(TEST_WEBSOCKET_HEADER_KEY, TEST_WEBSOCKET_HEADER_VALUE);
        options.webSocketMaxFrameSize(TEST_WEBSOCKET_MAX_FRAME_SIZE);

        return options;
    }
}
