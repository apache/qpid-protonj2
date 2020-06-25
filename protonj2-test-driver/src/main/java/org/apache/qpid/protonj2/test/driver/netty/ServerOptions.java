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
package org.apache.qpid.protonj2.test.driver.netty;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;

/**
 * Encapsulates all the Transport options in one configuration object.
 */
public class ServerOptions implements Cloneable {

    private static final int SERVER_CHOOSES_PORT = 0;

    public static final int DEFAULT_SEND_BUFFER_SIZE = 64 * 1024;
    public static final int DEFAULT_RECEIVE_BUFFER_SIZE = DEFAULT_SEND_BUFFER_SIZE;
    public static final int DEFAULT_TRAFFIC_CLASS = 0;
    public static final boolean DEFAULT_TCP_NO_DELAY = true;
    public static final boolean DEFAULT_TCP_KEEP_ALIVE = false;
    public static final int DEFAULT_SO_LINGER = Integer.MIN_VALUE;
    public static final int DEFAULT_SO_TIMEOUT = -1;
    public static final int DEFAULT_SERVER_PORT = SERVER_CHOOSES_PORT;
    public static final boolean DEFAULT_TRACE_BYTES = false;
    public static final String DEFAULT_STORE_TYPE = "jks";
    public static final String DEFAULT_CONTEXT_PROTOCOL = "TLS";
    public static final boolean DEFAULT_TRUST_ALL = false;
    public static final boolean DEFAULT_VERIFY_HOST = true;
    public static final List<String> DEFAULT_DISABLED_PROTOCOLS = Collections.unmodifiableList(Arrays.asList(new String[]{"SSLv2Hello", "SSLv3"}));
    public static final int DEFAULT_LOCAL_PORT = 0;
    public static final boolean DEFAULT_USE_WEBSOCKETS = false;
    public static final boolean DEFAULT_SECURE_SERVER = false;
    public static final boolean DEFAULT_NEEDS_CLIENT_AUTH = false;

    private static final String JAVAX_NET_SSL_KEY_STORE = "javax.net.ssl.keyStore";
    private static final String JAVAX_NET_SSL_KEY_STORE_TYPE = "javax.net.ssl.keyStoreType";
    private static final String JAVAX_NET_SSL_KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
    private static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    private static final String JAVAX_NET_SSL_TRUST_STORE_TYPE = "javax.net.ssl.trustStoreType";
    private static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

    private int sendBufferSize = DEFAULT_SEND_BUFFER_SIZE;
    private int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;
    private int trafficClass = DEFAULT_TRAFFIC_CLASS;
    private int soTimeout = DEFAULT_SO_TIMEOUT;
    private int soLinger = DEFAULT_SO_LINGER;
    private boolean tcpKeepAlive = DEFAULT_TCP_KEEP_ALIVE;
    private boolean tcpNoDelay = DEFAULT_TCP_NO_DELAY;
    private int serverPort = DEFAULT_SERVER_PORT;
    private String localAddress;
    private int localPort = DEFAULT_LOCAL_PORT;
    private boolean traceBytes = DEFAULT_TRACE_BYTES;
    private boolean useWebSockets = DEFAULT_USE_WEBSOCKETS;

    private boolean secure = DEFAULT_SECURE_SERVER;
    private boolean needClientAuth = DEFAULT_NEEDS_CLIENT_AUTH;
    private String keyStoreLocation;
    private String keyStorePassword;
    private String trustStoreLocation;
    private String trustStorePassword;
    private String keyStoreType;
    private String trustStoreType;
    private String[] enabledCipherSuites;
    private String[] disabledCipherSuites;
    private String[] enabledProtocols;
    private String[] disabledProtocols = DEFAULT_DISABLED_PROTOCOLS.toArray(new String[0]);
    private String contextProtocol = DEFAULT_CONTEXT_PROTOCOL;

    private boolean trustAll = DEFAULT_TRUST_ALL;
    private boolean verifyHost = DEFAULT_VERIFY_HOST;
    private String keyAlias;
    private SSLContext sslContextOverride;

    private final Map<String, String> httpHeaders = new HashMap<>();

    public ServerOptions() {
        setKeyStoreLocation(System.getProperty(JAVAX_NET_SSL_KEY_STORE));
        setKeyStoreType(System.getProperty(JAVAX_NET_SSL_KEY_STORE_TYPE, DEFAULT_STORE_TYPE));
        setKeyStorePassword(System.getProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD));
        setTrustStoreLocation(System.getProperty(JAVAX_NET_SSL_TRUST_STORE));
        setTrustStoreType(System.getProperty(JAVAX_NET_SSL_TRUST_STORE_TYPE, DEFAULT_STORE_TYPE));
        setTrustStorePassword(System.getProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD));
    }

    @Override
    public ServerOptions clone() {
        return copyOptions(new ServerOptions());
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
     * @throws IllegalArgumentException if the value given is not in the valid range.
     */
    public void setSendBufferSize(int sendBufferSize) {
        if (sendBufferSize <= 0) {
            throw new IllegalArgumentException("The send buffer size must be > 0");
        }

        this.sendBufferSize = sendBufferSize;
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
     * @throws IllegalArgumentException if the value given is not in the valid range.
     */
    public void setReceiveBufferSize(int receiveBufferSize) {
        if (receiveBufferSize <= 0) {
            throw new IllegalArgumentException("The send buffer size must be > 0");
        }

        this.receiveBufferSize = receiveBufferSize;
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
     * @throws IllegalArgumentException if the value given is not in the valid range.
     */
    public void setTrafficClass(int trafficClass) {
        if (trafficClass < 0 || trafficClass > 255) {
            throw new IllegalArgumentException("Traffic class must be in the range [0..255]");
        }

        this.trafficClass = trafficClass;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public int getSoLinger() {
        return soLinger;
    }

    public void setSoLinger(int soLinger) {
        this.soLinger = soLinger;
    }

    public boolean isTcpKeepAlive() {
        return tcpKeepAlive;
    }

    public void setTcpKeepAlive(boolean keepAlive) {
        this.tcpKeepAlive = keepAlive;
    }

    public int getServerPort() {
        return serverPort;
    }

    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    public String getLocalAddress() {
        return localAddress;
    }

    public void setLocalAddress(String localAddress) {
        this.localAddress = localAddress;
    }

    public int getLocalPort() {
        return localPort;
    }

    public void setLocalPort(int localPort) {
        this.localPort = localPort;
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
     */
    public void setTraceBytes(boolean traceBytes) {
        this.traceBytes = traceBytes;
    }

    /**
     * @return the keyStoreLocation currently configured.
     */
    public String getKeyStoreLocation() {
        return keyStoreLocation;
    }

    /**
     * Sets the location on disk of the key store to use.
     *
     * @param keyStoreLocation
     *        the keyStoreLocation to use to create the key manager.
     */
    public void setKeyStoreLocation(String keyStoreLocation) {
        this.keyStoreLocation = keyStoreLocation;
    }

    /**
     * @return the keyStorePassword
     */
    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    /**
     * @param keyStorePassword the keyStorePassword to set
     */
    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    /**
     * @return the trustStoreLocation
     */
    public String getTrustStoreLocation() {
        return trustStoreLocation;
    }

    /**
     * @param trustStoreLocation the trustStoreLocation to set
     */
    public void setTrustStoreLocation(String trustStoreLocation) {
        this.trustStoreLocation = trustStoreLocation;
    }

    /**
     * @return the trustStorePassword
     */
    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    /**
     * @param trustStorePassword the trustStorePassword to set
     */
    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    /**
     * @param storeType
     *        the format that the store files are encoded in.
     */
    public void setStoreType(String storeType) {
        setKeyStoreType(storeType);
        setTrustStoreType(storeType);
    }

    /**
     * @return the keyStoreType
     */
    public String getKeyStoreType() {
        return keyStoreType;
    }

    /**
     * @param keyStoreType
     *        the format that the keyStore file is encoded in
     */
    public void setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
    }

    /**
     * @return the trustStoreType
     */
    public String getTrustStoreType() {
        return trustStoreType;
    }

    /**
     * @param trustStoreType
     *        the format that the trustStore file is encoded in
     */
    public void setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
    }

    /**
     * @return the enabledCipherSuites
     */
    public String[] getEnabledCipherSuites() {
        return enabledCipherSuites;
    }

    /**
     * @param enabledCipherSuites the enabledCipherSuites to set
     */
    public void setEnabledCipherSuites(String[] enabledCipherSuites) {
        this.enabledCipherSuites = enabledCipherSuites;
    }

    /**
     * @return the disabledCipherSuites
     */
    public String[] getDisabledCipherSuites() {
        return disabledCipherSuites;
    }

    /**
     * @param disabledCipherSuites the disabledCipherSuites to set
     */
    public void setDisabledCipherSuites(String[] disabledCipherSuites) {
        this.disabledCipherSuites = disabledCipherSuites;
    }

    /**
     * @return the enabledProtocols or null if the defaults should be used
     */
    public String[] getEnabledProtocols() {
        return enabledProtocols;
    }

    /**
     * The protocols to be set as enabled.
     *
     * @param enabledProtocols the enabled protocols to set, or null if the defaults should be used.
     */
    public void setEnabledProtocols(String[] enabledProtocols) {
        this.enabledProtocols = enabledProtocols;
    }

    /**
     * @return the protocols to disable or null if none should be
     */
    public String[] getDisabledProtocols() {
        return disabledProtocols;
    }

    /**
     * The protocols to be disable.
     *
     * @param disabledProtocols the protocols to disable, or null if none should be.
     */
    public void setDisabledProtocols(String[] disabledProtocols) {
        this.disabledProtocols = disabledProtocols;
    }

    /**
    * @return the context protocol to use
    */
    public String getContextProtocol() {
        return contextProtocol;
    }

    /**
     * The protocol value to use when creating an SSLContext via
     * SSLContext.getInstance(protocol).
     *
     * @param contextProtocol the context protocol to use.
     */
    public void setContextProtocol(String contextProtocol) {
        this.contextProtocol = contextProtocol;
    }

    /**
     * @return the trustAll
     */
    public boolean isTrustAll() {
        return trustAll;
    }

    /**
     * @param trustAll the trustAll to set
     */
    public void setTrustAll(boolean trustAll) {
        this.trustAll = trustAll;
    }

    /**
     * @return the verifyHost
     */
    public boolean isVerifyHost() {
        return verifyHost;
    }

    /**
     * @param verifyHost the verifyHost to set
     */
    public void setVerifyHost(boolean verifyHost) {
        this.verifyHost = verifyHost;
    }

    /**
     * @return the key alias
     */
    public String getKeyAlias() {
        return keyAlias;
    }

    /**
     * @param keyAlias the key alias to use
     */
    public void setKeyAlias(String keyAlias) {
        this.keyAlias = keyAlias;
    }

    public void setSslContextOverride(SSLContext sslContextOverride) {
        this.sslContextOverride = sslContextOverride;
    }

    public SSLContext getSslContextOverride() {
        return sslContextOverride;
    }

    public Map<String, String> getHttpHeaders() {
        return httpHeaders;
    }

    /**
     * @return the configuration that controls if the server requires client authentication.
     */
    public boolean isNeedClientAuth() {
        return needClientAuth;
    }

    /**
     * @param needClientAuth
     *      the needClientAuth should the server require client authentication.
     */
    public void setNeedClientAuth(boolean needClientAuth) {
        this.needClientAuth = needClientAuth;
    }

    /**
     * @return the true if this server requires SSL connections
     */
    public boolean isSecure() {
        return secure;
    }

    /**
     * @param secure
     *      should the sever require SSL connections.
     */
    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    /**
     * @return true if this server operates over a WebSocket connection.
     */
    public boolean isUseWebSockets() {
        return useWebSockets;
    }

    /**
     * @param useWebSockets
     *      Is this a WebSocket based server.
     */
    public void setUseWebSockets(boolean useWebSockets) {
        this.useWebSockets = useWebSockets;
    }

    protected ServerOptions copyOptions(ServerOptions copy) {
        copy.setReceiveBufferSize(getReceiveBufferSize());
        copy.setSendBufferSize(getSendBufferSize());
        copy.setSoLinger(getSoLinger());
        copy.setSoTimeout(getSoTimeout());
        copy.setTcpKeepAlive(isTcpKeepAlive());
        copy.setTcpNoDelay(isTcpNoDelay());
        copy.setTrafficClass(getTrafficClass());
        copy.setServerPort(getServerPort());
        copy.setTraceBytes(isTraceBytes());
        copy.setKeyStoreLocation(getKeyStoreLocation());
        copy.setKeyStorePassword(getKeyStorePassword());
        copy.setTrustStoreLocation(getTrustStoreLocation());
        copy.setTrustStorePassword(getTrustStorePassword());
        copy.setKeyStoreType(getKeyStoreType());
        copy.setTrustStoreType(getTrustStoreType());
        copy.setEnabledCipherSuites(getEnabledCipherSuites());
        copy.setDisabledCipherSuites(getDisabledCipherSuites());
        copy.setEnabledProtocols(getEnabledProtocols());
        copy.setDisabledProtocols(getDisabledProtocols());
        copy.setTrustAll(isTrustAll());
        copy.setVerifyHost(isVerifyHost());
        copy.setKeyAlias(getKeyAlias());
        copy.setContextProtocol(getContextProtocol());
        copy.setSslContextOverride(getSslContextOverride());
        copy.setLocalAddress(getLocalAddress());
        copy.setLocalPort(getLocalPort());
        copy.setSecure(isSecure());
        copy.setNeedClientAuth(isNeedClientAuth());
        copy.setUseWebSockets(isUseWebSockets());

        return copy;
    }
}
