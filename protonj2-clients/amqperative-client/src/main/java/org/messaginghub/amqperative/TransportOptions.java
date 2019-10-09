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

import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates all the Transport options in one configuration object.
 */
public class TransportOptions implements Cloneable {

    public static final int DEFAULT_SEND_BUFFER_SIZE = 64 * 1024;
    public static final int DEFAULT_RECEIVE_BUFFER_SIZE = DEFAULT_SEND_BUFFER_SIZE;
    public static final int DEFAULT_TRAFFIC_CLASS = 0;
    public static final boolean DEFAULT_TCP_NO_DELAY = true;
    public static final boolean DEFAULT_TCP_KEEP_ALIVE = false;
    public static final int DEFAULT_SO_LINGER = Integer.MIN_VALUE;
    public static final int DEFAULT_SO_TIMEOUT = -1;
    public static final int DEFAULT_CONNECT_TIMEOUT = 60000;
    public static final int DEFAULT_TCP_PORT = 5672;
    public static final boolean DEFAULT_ALLOW_NATIVE_IO = true;
    public static final boolean DEFAULT_TRACE_BYTES = false;
    public static final int DEFAULT_LOCAL_PORT = 0;
    public static final boolean DEFAULT_USE_WEBSOCKETS = false;

    private int sendBufferSize = DEFAULT_SEND_BUFFER_SIZE;
    private int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;
    private int trafficClass = DEFAULT_TRAFFIC_CLASS;
    private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private int soTimeout = DEFAULT_SO_TIMEOUT;
    private int soLinger = DEFAULT_SO_LINGER;
    private boolean tcpKeepAlive = DEFAULT_TCP_KEEP_ALIVE;
    private boolean tcpNoDelay = DEFAULT_TCP_NO_DELAY;
    private int defaultTcpPort = DEFAULT_TCP_PORT;
    private String localAddress;
    private int localPort = DEFAULT_LOCAL_PORT;
    private boolean allowNativeIO = DEFAULT_ALLOW_NATIVE_IO;
    private boolean traceBytes = DEFAULT_TRACE_BYTES;
    private boolean useWebSockets = DEFAULT_USE_WEBSOCKETS;
    private String webSocketPath;

    private final Map<String, String> httpHeaders = new HashMap<>();

    @Override
    public TransportOptions clone() {
        return copyInto(new TransportOptions());
    }

    /**
     * @return the currently set send buffer size in bytes.
     */
    public int getSendBufferSize() {
        return sendBufferSize;
    }

    /**
     * Sets the send buffer size in bytes, the value must be greater than zero
     * or an {@link IllegalArgumentException} will be thrown.
     *
     * @param sendBufferSize
     *        the new send buffer size for the TCP Transport.
     *
     * @return this options instance.
     *
     * @throws IllegalArgumentException if the value given is not in the valid range.
     */
    public TransportOptions setSendBufferSize(int sendBufferSize) {
        if (sendBufferSize <= 0) {
            throw new IllegalArgumentException("The send buffer size must be > 0");
        }

        this.sendBufferSize = sendBufferSize;
        return this;
    }

    /**
     * @return the currently configured receive buffer size in bytes.
     */
    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    /**
     * Sets the receive buffer size in bytes, the value must be greater than zero
     * or an {@link IllegalArgumentException} will be thrown.
     *
     * @param receiveBufferSize
     *        the new receive buffer size for the TCP Transport.
     *
     * @return this options instance.
     *
     * @throws IllegalArgumentException if the value given is not in the valid range.
     */
    public TransportOptions setReceiveBufferSize(int receiveBufferSize) {
        if (receiveBufferSize <= 0) {
            throw new IllegalArgumentException("The send buffer size must be > 0");
        }

        this.receiveBufferSize = receiveBufferSize;
        return this;
    }

    /**
     * @return the currently configured traffic class value.
     */
    public int getTrafficClass() {
        return trafficClass;
    }

    /**
     * Sets the traffic class value used by the TCP connection, valid
     * range is between 0 and 255.
     *
     * @param trafficClass
     *        the new traffic class value.
     *
     * @return this options instance.
     *
     * @throws IllegalArgumentException if the value given is not in the valid range.
     */
    public TransportOptions setTrafficClass(int trafficClass) {
        if (trafficClass < 0 || trafficClass > 255) {
            throw new IllegalArgumentException("Traffic class must be in the range [0..255]");
        }

        this.trafficClass = trafficClass;
        return this;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public TransportOptions setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
        return this;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public TransportOptions setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    public int getSoLinger() {
        return soLinger;
    }

    public TransportOptions setSoLinger(int soLinger) {
        this.soLinger = soLinger;
        return this;
    }

    public boolean isTcpKeepAlive() {
        return tcpKeepAlive;
    }

    public TransportOptions setTcpKeepAlive(boolean keepAlive) {
        this.tcpKeepAlive = keepAlive;
        return this;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public TransportOptions setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public int getDefaultTcpPort() {
        return defaultTcpPort;
    }

    public TransportOptions setDefaultTcpPort(int defaultTcpPort) {
        this.defaultTcpPort = defaultTcpPort;
        return this;
    }

    public String getLocalAddress() {
        return localAddress;
    }

    public TransportOptions setLocalAddress(String localAddress) {
        this.localAddress = localAddress;
        return this;
    }

    public int getLocalPort() {
        return localPort;
    }

    public TransportOptions setLocalPort(int localPort) {
        this.localPort = localPort;
        return this;
    }

    /**
     * @return true if an native IO library can be used if available on this platform instead of the JDK IO.
     */
    public boolean isAllowNativeIO() {
        return allowNativeIO;
    }

    /**
     * Determines if the a native IO implementation is preferred to the JDK based IO.
     *
     * @param allowNativeIO
     * 		should use of available native transport be allowed if one is available.
     *
     * @return this options instance.
     */
    public TransportOptions setAllowNativeIO(boolean allowNativeIO) {
        this.allowNativeIO = allowNativeIO;
        return this;
    }

    /**
     * @return true if the transport should enable byte tracing
     */
    public boolean isTraceBytes() {
        return traceBytes;
    }

    /**
     * Determines if the transport should add a logger for bytes in / out
     *
     * @param traceBytes
     * 		should the transport log the bytes in and out.
     *
     * @return this options instance.
     */
    public TransportOptions setTraceBytes(boolean traceBytes) {
        this.traceBytes = traceBytes;
        return this;
    }

    public Map<String, String> getHttpHeaders() {
        return httpHeaders;
    }

    public boolean isUseWebSockets() {
        return useWebSockets;
    }

    public TransportOptions setUseWebSockets(boolean webSockets) {
        this.useWebSockets = webSockets;
        return this;
    }

    public String getWebSocketPath() {
        return webSocketPath;
    }

    public TransportOptions setWebSocketPath(String webSocketPath) {
        this.webSocketPath = webSocketPath;
        return this;
    }

    /**
     * Copy all configuration into the given {@link TransportOptions} from this instance.
     *
     * @param other
     * 		another {@link TransportOptions} instance that will receive the configuration from this instance.
     *
     * @return the options instance that was copied into.
     */
    public TransportOptions copyInto(TransportOptions other) {
        other.setConnectTimeout(getConnectTimeout());
        other.setReceiveBufferSize(getReceiveBufferSize());
        other.setSendBufferSize(getSendBufferSize());
        other.setSoLinger(getSoLinger());
        other.setSoTimeout(getSoTimeout());
        other.setTcpKeepAlive(isTcpKeepAlive());
        other.setTcpNoDelay(isTcpNoDelay());
        other.setTrafficClass(getTrafficClass());
        other.setDefaultTcpPort(getDefaultTcpPort());
        other.setAllowNativeIO(isAllowNativeIO());
        other.setTraceBytes(isTraceBytes());
        other.setLocalAddress(getLocalAddress());
        other.setLocalPort(getLocalPort());
        other.setUseWebSockets(isUseWebSockets());
        other.setWebSocketPath(getWebSocketPath());

        return other;
    }
}
