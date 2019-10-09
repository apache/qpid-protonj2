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
package org.messaginghub.amqperative;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.messaginghub.amqperative.test.AMQPerativeTestCase;

/**
 * Test for class TransportOptions
 */
public class TransportOptionsTest extends AMQPerativeTestCase {

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

    @Test
    public void testCreate() {
        TransportOptions options = new TransportOptions();

        assertEquals(TransportOptions.DEFAULT_TCP_NO_DELAY, options.isTcpNoDelay());

        assertTrue(options.isAllowNativeIO());
        assertFalse(options.isUseWebSockets());
        assertNull(options.getWebSocketPath());
    }

    @Test
    public void testOptions() {
        TransportOptions options = createNonDefaultOptions();

        assertEquals(TEST_SEND_BUFFER_SIZE, options.getSendBufferSize());
        assertEquals(TEST_RECEIVE_BUFFER_SIZE, options.getReceiveBufferSize());
        assertEquals(TEST_TRAFFIC_CLASS, options.getTrafficClass());
        assertEquals(TEST_TCP_NO_DELAY, options.isTcpNoDelay());
        assertEquals(TEST_TCP_KEEP_ALIVE, options.isTcpKeepAlive());
        assertEquals(TEST_SO_LINGER, options.getSoLinger());
        assertEquals(TEST_SO_TIMEOUT, options.getSoTimeout());
        assertEquals(TEST_CONNECT_TIMEOUT, options.getConnectTimeout());
        assertEquals(TEST_DEFAULT_TCP_PORT, options.getDefaultTcpPort());
        assertEquals(TEST_ALLOW_NATIVE_IO_VALUE, options.isAllowNativeIO());
        assertEquals(TEST_TRACE_BYTES_VALUE, options.isTraceBytes());
    }

    @Test
    public void testClone() {
        TransportOptions options = createNonDefaultOptions().clone();

        assertEquals(TEST_SEND_BUFFER_SIZE, options.getSendBufferSize());
        assertEquals(TEST_RECEIVE_BUFFER_SIZE, options.getReceiveBufferSize());
        assertEquals(TEST_TRAFFIC_CLASS, options.getTrafficClass());
        assertEquals(TEST_TCP_NO_DELAY, options.isTcpNoDelay());
        assertEquals(TEST_TCP_KEEP_ALIVE, options.isTcpKeepAlive());
        assertEquals(TEST_SO_LINGER, options.getSoLinger());
        assertEquals(TEST_SO_TIMEOUT, options.getSoTimeout());
        assertEquals(TEST_CONNECT_TIMEOUT, options.getConnectTimeout());
        assertEquals(TEST_DEFAULT_TCP_PORT, options.getDefaultTcpPort());
        assertEquals(TEST_ALLOW_NATIVE_IO_VALUE, options.isAllowNativeIO());
        assertEquals(TEST_TRACE_BYTES_VALUE, options.isTraceBytes());
        assertEquals(LOCAL_ADDRESS,options.getLocalAddress());
        assertEquals(LOCAL_PORT,options.getLocalPort());
        assertEquals(TEST_WEBSOCKET_PATH, options.getWebSocketPath());
    }

    @Test
    public void testSendBufferSizeValidation() {
        TransportOptions options = createNonDefaultOptions().clone();
        try {
            options.setSendBufferSize(0);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
        try {
            options.setSendBufferSize(-1);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }

        options.setSendBufferSize(1);
    }

    @Test
    public void testReceiveBufferSizeValidation() {
        TransportOptions options = createNonDefaultOptions().clone();
        try {
            options.setReceiveBufferSize(0);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
        try {
            options.setReceiveBufferSize(-1);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }

        options.setReceiveBufferSize(1);
    }

    @Test
    public void testTrafficClassValidation() {
        TransportOptions options = createNonDefaultOptions().clone();
        try {
            options.setTrafficClass(-1);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
        try {
            options.setTrafficClass(256);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }

        options.setTrafficClass(0);
        options.setTrafficClass(128);
        options.setTrafficClass(255);
    }

    @Test
    public void testCreateAndConfigure() {
        TransportOptions options = createNonDefaultOptions();

        assertEquals(TEST_SEND_BUFFER_SIZE, options.getSendBufferSize());
        assertEquals(TEST_RECEIVE_BUFFER_SIZE, options.getReceiveBufferSize());
        assertEquals(TEST_TRAFFIC_CLASS, options.getTrafficClass());
        assertEquals(TEST_TCP_NO_DELAY, options.isTcpNoDelay());
        assertEquals(TEST_TCP_KEEP_ALIVE, options.isTcpKeepAlive());
        assertEquals(TEST_SO_LINGER, options.getSoLinger());
        assertEquals(TEST_SO_TIMEOUT, options.getSoTimeout());
        assertEquals(TEST_CONNECT_TIMEOUT, options.getConnectTimeout());
    }

    private TransportOptions createNonDefaultOptions() {
        TransportOptions options = new TransportOptions();

        options.setSendBufferSize(TEST_SEND_BUFFER_SIZE);
        options.setReceiveBufferSize(TEST_RECEIVE_BUFFER_SIZE);
        options.setTrafficClass(TEST_TRAFFIC_CLASS);
        options.setTcpNoDelay(TEST_TCP_NO_DELAY);
        options.setTcpKeepAlive(TEST_TCP_KEEP_ALIVE);
        options.setSoLinger(TEST_SO_LINGER);
        options.setSoTimeout(TEST_SO_TIMEOUT);
        options.setConnectTimeout(TEST_CONNECT_TIMEOUT);
        options.setDefaultTcpPort(TEST_DEFAULT_TCP_PORT);
        options.setAllowNativeIO(TEST_ALLOW_NATIVE_IO_VALUE);
        options.setTraceBytes(TEST_TRACE_BYTES_VALUE);
        options.setLocalAddress(LOCAL_ADDRESS);
        options.setLocalPort(LOCAL_PORT);
        options.setWebSocketPath(TEST_WEBSOCKET_PATH);

        return options;
    }
}
