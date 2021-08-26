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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
    public static final int DEFAULT_WEBSOCKET_MAX_FRAME_SIZE = 65535;
    private static final String[] DEFAULT_NATIVEIO_PREFERENCES_ARRAY = { "EPOLL", "KQUEUE" };
    public static final List<String> DEFAULT_NATIVEIO_PREFERENCES =
        Collections.unmodifiableList(Arrays.asList( DEFAULT_NATIVEIO_PREFERENCES_ARRAY ));

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
    private String[] nativeIOPreference = DEFAULT_NATIVEIO_PREFERENCES_ARRAY;
    private boolean traceBytes = DEFAULT_TRACE_BYTES;
    private boolean useWebSockets = DEFAULT_USE_WEBSOCKETS;
    private String webSocketPath;
    private int webSocketMaxFrameSize = DEFAULT_WEBSOCKET_MAX_FRAME_SIZE;

    private final Map<String, String> webSocketHeaders = new HashMap<>();

    @Override
    public TransportOptions clone() {
        return copyInto(new TransportOptions());
    }

    /**
     * @return the currently set send buffer size in bytes.
     */
    public int sendBufferSize() {
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
    public TransportOptions sendBufferSize(int sendBufferSize) {
        if (sendBufferSize <= 0) {
            throw new IllegalArgumentException("The send buffer size must be > 0");
        }

        this.sendBufferSize = sendBufferSize;
        return this;
    }

    /**
     * @return the currently configured receive buffer size in bytes.
     */
    public int receiveBufferSize() {
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
    public TransportOptions receiveBufferSize(int receiveBufferSize) {
        if (receiveBufferSize <= 0) {
            throw new IllegalArgumentException("The send buffer size must be > 0");
        }

        this.receiveBufferSize = receiveBufferSize;
        return this;
    }

    /**
     * @return the currently configured traffic class value.
     */
    public int trafficClass() {
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
    public TransportOptions trafficClass(int trafficClass) {
        if (trafficClass < 0 || trafficClass > 255) {
            throw new IllegalArgumentException("Traffic class must be in the range [0..255]");
        }

        this.trafficClass = trafficClass;
        return this;
    }

    /**
     * @return the configured value for the socket level soTimeout option.
     */
    public int soTimeout() {
        return soTimeout;
    }

    /**
     * Sets the value to use when configuring the socket level soTimeout option.
     *
     * @param soTimeout
     * 		the soTimeout value to configure on the newly create socket.
     *
     * @return this {@link TransportOptions} instance.
     */
    public TransportOptions soTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
        return this;
    }

    /**
     * @return the configured value for the socket level tcpNoDelay option.
     */
    public boolean tcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * Configure the underlying connection to use the tcpNoDelay option.
     *
     * @param tcpNoDelay
     * 		controls if the underlying connection configures tcpNoDelay.
     *
     * @return this {@link TransportOptions} instance.
     */
    public TransportOptions tcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    /**
     * @return the configured value for the socket level soLinger option.
     */
    public int soLinger() {
        return soLinger;
    }

    /**
     * Configures the soLinger value that should be configured on the connection socket.
     *
     * @param soLinger
     * 		The soLinger value to use for the underlying socket.
     *
     * @return this {@link TransportOptions} instance.
     */
    public TransportOptions soLinger(int soLinger) {
        this.soLinger = soLinger;
        return this;
    }

    /**
     * @return the configured value for the socket level tcpKeepAlive option.
     */
    public boolean tcpKeepAlive() {
        return tcpKeepAlive;
    }

    /**
     * Configures the TCP socket keep-alive value that should be used for a connection.
     *
     * @param keepAlive
     * 		The TCP/IP keep-alive value to use for the connection.
     *
     * @return this {@link TransportOptions} instance.
     */
    public TransportOptions tcpKeepAlive(boolean keepAlive) {
        this.tcpKeepAlive = keepAlive;
        return this;
    }

    /**
     * @return the configured value for the socket level connection timeout option.
     */
    public int connectTimeout() {
        return connectTimeout;
    }

    /**
     * A transport layer connection timeout value that controls how long a connection attempt can block
     * before giving up and throwing a connection error.
     *
     * @param connectTimeout
     * 		connection timeout in milliseconds.
     *
     * @return this {@link TransportOptions} instance.
     */
    public TransportOptions connectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    /**
     * @return the configured value for the socket level default port to use for connection if none is provided.
     */
    public int defaultTcpPort() {
        return defaultTcpPort;
    }

    /**
     * The default unsecured connection port to use when none has been specified by the user.
     *
     * @param defaultTcpPort
     * 		the default port to use for non-SSL connection if none provided.
     *
     * @return this {@link TransportOptions} instance.
     */
    public TransportOptions defaultTcpPort(int defaultTcpPort) {
        this.defaultTcpPort = defaultTcpPort;
        return this;
    }

    /**
     * @return the configured value for the socket level localAddress option.
     */
    public String localAddress() {
        return localAddress;
    }

    /**
     * The local address to bind to when attempting a remote connection.
     *
     * @param localAddress
     * 		the local address to bind to.
     *
     * @return this {@link TransportOptions} instance.
     */
    public TransportOptions localAddress(String localAddress) {
        this.localAddress = localAddress;
        return this;
    }

    /**
     * @return the configured value for the socket level localPort option.
     */
    public int localPort() {
        return localPort;
    }

    /**
     * Configures the local port value to bind to when connecting to the remote, if that port is
     * already in use this can lead to a connection error.
     *
     * @param localPort
     * 		local port value to use when making a remote connection.
     *
     * @return this {@link TransportOptions} instance.
     */
    public TransportOptions localPort(int localPort) {
        this.localPort = localPort;
        return this;
    }

    /**
     * @return true if an native IO library can be used if available on this platform instead of the JDK IO.
     */
    public boolean allowNativeIO() {
        return allowNativeIO;
    }

    /**
     * Determines if the a native IO implementation is preferred to the JDK based IO.
     *
     * @param allowNativeIO
     * 		should use of available native transport be allowed if one is available.
     *
     * @return this {@link TransportOptions} instance.
     */
    public TransportOptions allowNativeIO(boolean allowNativeIO) {
        this.allowNativeIO = allowNativeIO;
        return this;
    }

    /**
     * @return the nativeIOPreference
     */
    public String[] nativeIOPreference() {
        return nativeIOPreference;
    }

    /**
     * @param nativeIOPreference the nativeIOPreference to set
     */
    public void nativeIOPreference(String... nativeIOPreference) {
        if (nativeIOPreference == null || nativeIOPreference.length == 0 || nativeIOPreference.length == 1 && nativeIOPreference[0] == null) {
            this.nativeIOPreference = DEFAULT_NATIVEIO_PREFERENCES_ARRAY;
        } else {
            this.nativeIOPreference = nativeIOPreference;
        }
    }

    /**
     * @return true if the transport should enable byte tracing
     */
    public boolean traceBytes() {
        return traceBytes;
    }

    /**
     * Determines if the transport should add a logger for bytes in / out
     *
     * @param traceBytes
     * 		should the transport log the bytes in and out.
     *
     * @return this {@link TransportOptions} instance.
     */
    public TransportOptions traceBytes(boolean traceBytes) {
        this.traceBytes = traceBytes;
        return this;
    }

    /**
     * @return the configured state of WebSockets for client connections.
     */
    public boolean useWebSockets() {
        return useWebSockets;
    }

    /**
     * Set to true to configure the transport layer as a WebSocket based connection.
     *
     * @param webSockets
     * 		should the transport attempt connection using a WebSocket.
     *
     * @return this {@link TransportOptions} instance.
     */
    public TransportOptions useWebSockets(boolean webSockets) {
        this.useWebSockets = webSockets;
        return this;
    }

    /**
     * @return the configured value for the socket level WebSocket path option.
     */
    public String webSocketPath() {
        return webSocketPath;
    }

    /**
     * The optional path element to provide when connecting via a WebSocket.
     *
     * @param webSocketPath
     * 		path value to use for WebSocket connections.
     *
     * @return this {@link TransportOptions} instance.
     */
    public TransportOptions webSocketPath(String webSocketPath) {
        this.webSocketPath = webSocketPath;
        return this;
    }

    /**
     * @return the configured value for the socket level WebSocket Headers option.
     */
    public Map<String, String> webSocketHeaders() {
        return webSocketHeaders;
    }

    /**
     * Additional headers to be passed along to the remote when performing a WebSocket based connect.
     *
     * @param key
     * 		the header key to use with the provided value.
     * @param value
     *      the value to store under the provided key.
     *
     * @return this {@link TransportOptions} instance.
     */
    public TransportOptions addWebSocketHeader(String key, String value) {
        this.webSocketHeaders.put(key, value);
        return this;
    }

    /**
     * @return the configured value for the socket level WebSocket max frame size option.
     */
    public int webSocketMaxFrameSize() {
        return webSocketMaxFrameSize;
    }

    /**
     * Configures the maximum size of each WebSocket frame, payloads larger than the max frame size are
     * split into multiple frames during transmission.
     *
     * @param maxFrameSize
     * 		the maximum WebSocket frame size before payloads are split.
     *
     * @return this {@link TransportOptions} instance.
     */
    public TransportOptions webSocketMaxFrameSize(int maxFrameSize) {
        this.webSocketMaxFrameSize = maxFrameSize;
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
        other.connectTimeout(connectTimeout());
        other.receiveBufferSize(receiveBufferSize());
        other.sendBufferSize(sendBufferSize());
        other.soLinger(soLinger());
        other.soTimeout(soTimeout());
        other.tcpKeepAlive(tcpKeepAlive());
        other.tcpNoDelay(tcpNoDelay());
        other.trafficClass(trafficClass());
        other.defaultTcpPort(defaultTcpPort());
        other.allowNativeIO(allowNativeIO());
        other.nativeIOPreference(nativeIOPreference());
        other.traceBytes(traceBytes());
        other.localAddress(localAddress());
        other.localPort(localPort());
        other.useWebSockets(useWebSockets());
        other.webSocketPath(webSocketPath());
        other.webSocketHeaders().putAll(webSocketHeaders);
        other.webSocketMaxFrameSize(webSocketMaxFrameSize());

        return other;
    }
}
