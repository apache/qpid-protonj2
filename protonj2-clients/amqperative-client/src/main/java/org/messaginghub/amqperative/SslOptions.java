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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.net.ssl.SSLContext;

/**
 * Options for configuration of the client SSL layer
 */
public class SslOptions implements Cloneable {

    public static final String DEFAULT_STORE_TYPE = "jks";
    public static final String DEFAULT_CONTEXT_PROTOCOL = "TLS";
    public static final boolean DEFAULT_TRUST_ALL = false;
    public static final boolean DEFAULT_VERIFY_HOST = true;
    public static final List<String> DEFAULT_DISABLED_PROTOCOLS = Collections.unmodifiableList(Arrays.asList(new String[]{"SSLv2Hello", "SSLv3"}));
    public static final int DEFAULT_SSL_PORT = 5671;
    public static final boolean DEFAULT_ALLOW_NATIVE_SSL = false;

    private static final String JAVAX_NET_SSL_KEY_STORE = "javax.net.ssl.keyStore";
    private static final String JAVAX_NET_SSL_KEY_STORE_TYPE = "javax.net.ssl.keyStoreType";
    private static final String JAVAX_NET_SSL_KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
    private static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    private static final String JAVAX_NET_SSL_TRUST_STORE_TYPE = "javax.net.ssl.trustStoreType";
    private static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

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
    private int defaultSslPort = DEFAULT_SSL_PORT;
    private SSLContext sslContextOverride;
    private boolean sslEnabled;
    private boolean allowNativeSSL = DEFAULT_ALLOW_NATIVE_SSL;

    public SslOptions() {
        setKeyStoreLocation(System.getProperty(JAVAX_NET_SSL_KEY_STORE));
        setKeyStoreType(System.getProperty(JAVAX_NET_SSL_KEY_STORE_TYPE, DEFAULT_STORE_TYPE));
        setKeyStorePassword(System.getProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD));
        setTrustStoreLocation(System.getProperty(JAVAX_NET_SSL_TRUST_STORE));
        setTrustStoreType(System.getProperty(JAVAX_NET_SSL_TRUST_STORE_TYPE, DEFAULT_STORE_TYPE));
        setTrustStorePassword(System.getProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD));
    }

    @Override
    public SslOptions clone() {
        return copyInto(new SslOptions());
    }

    public boolean isSSLEnabled() {
        return sslEnabled;
    }

    public SslOptions setSSLEnabled(boolean enable) {
        this.sslEnabled = enable;
        return this;
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
     *
     * @return this options instance.
     */
    public SslOptions setKeyStoreLocation(String keyStoreLocation) {
        this.keyStoreLocation = keyStoreLocation;
        return this;
    }

    /**
     * @return the keyStorePassword
     */
    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    /**
     * @param keyStorePassword the keyStorePassword to set
     *
     * @return this options instance.
     */
    public SslOptions setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    /**
     * @return the trustStoreLocation
     */
    public String getTrustStoreLocation() {
        return trustStoreLocation;
    }

    /**
     * @param trustStoreLocation the trustStoreLocation to set
     *
     * @return this options instance.
     */
    public SslOptions setTrustStoreLocation(String trustStoreLocation) {
        this.trustStoreLocation = trustStoreLocation;
        return this;
    }

    /**
     * @return the trustStorePassword
     */
    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    /**
     * @param trustStorePassword the trustStorePassword to set
     *
     * @return this options instance.
     */
    public SslOptions setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    /**
     * @param storeType
     *        the format that the store files are encoded in.
     *
     * @return this options instance.
     */
    public SslOptions setStoreType(String storeType) {
        setKeyStoreType(storeType);
        setTrustStoreType(storeType);
        return this;
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
     *
     * @return this options instance.
     */
    public SslOptions setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
        return this;
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
     *
     * @return this options instance.
     */
    public SslOptions setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
        return this;
    }

    /**
     * @return the enabledCipherSuites
     */
    public String[] getEnabledCipherSuites() {
        return enabledCipherSuites;
    }

    /**
     * @param enabledCipherSuites the enabledCipherSuites to set
     *
     * @return this options instance.
     */
    public SslOptions setEnabledCipherSuites(String[] enabledCipherSuites) {
        this.enabledCipherSuites = enabledCipherSuites;
        return this;
    }

    /**
     * @return the disabledCipherSuites
     */
    public String[] getDisabledCipherSuites() {
        return disabledCipherSuites;
    }

    /**
     * @param disabledCipherSuites the disabledCipherSuites to set
     *
     * @return this options instance.
     */
    public SslOptions setDisabledCipherSuites(String[] disabledCipherSuites) {
        this.disabledCipherSuites = disabledCipherSuites;
        return this;
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
     *
     * @return this options instance.
     */
    public SslOptions setEnabledProtocols(String[] enabledProtocols) {
        this.enabledProtocols = enabledProtocols;
        return this;
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
     *
     * @return this options instance.
     */
    public SslOptions setDisabledProtocols(String[] disabledProtocols) {
        this.disabledProtocols = disabledProtocols;
        return this;
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
     *
     * @return this options instance.
     */
    public SslOptions setContextProtocol(String contextProtocol) {
        this.contextProtocol = contextProtocol;
        return this;
    }

    /**
     * @return the trustAll
     */
    public boolean isTrustAll() {
        return trustAll;
    }

    /**
     * @param trustAll the trustAll to set
     *
     * @return this options instance.
     */
    public SslOptions setTrustAll(boolean trustAll) {
        this.trustAll = trustAll;
        return this;
    }

    /**
     * @return the verifyHost
     */
    public boolean isVerifyHost() {
        return verifyHost;
    }

    /**
     * @param verifyHost the verifyHost to set
     *
     * @return this options instance.
     */
    public SslOptions setVerifyHost(boolean verifyHost) {
        this.verifyHost = verifyHost;
        return this;
    }

    /**
     * @return the key alias
     */
    public String getKeyAlias() {
        return keyAlias;
    }

    /**
     * @param keyAlias the key alias to use
     *
     * @return this options instance.
     */
    public SslOptions setKeyAlias(String keyAlias) {
        this.keyAlias = keyAlias;
        return this;
    }

    public int getDefaultSslPort() {
        return defaultSslPort;
    }

    public SslOptions setDefaultSslPort(int defaultSslPort) {
        this.defaultSslPort = defaultSslPort;
        return this;
    }

    public SslOptions setSslContextOverride(SSLContext sslContextOverride) {
        this.sslContextOverride = sslContextOverride;
        return this;
    }

    public SSLContext getSslContextOverride() {
        return sslContextOverride;
    }

    /**
     * @return true if the an native SSL based encryption layer is allowed to be used instead of the JDK.
     */
    public boolean isAllowNativeSSL() {
        return allowNativeSSL;
    }

    /**
     * @param allowNativeSSL
     * 		Configure if the transport should attempt to use native SSL support if available.
     *
     * @return this options object.
     */
    public SslOptions setAllowNativeSSL(boolean allowNativeSSL) {
        this.allowNativeSSL = allowNativeSSL;
        return this;
    }

    /**
     * Copy all configuration into the given {@link SslOptions} from this instance.
     *
     * @param other
     * 		another {@link SslOptions} instance that will receive the configuration from this instance.
     *
     * @return the options instance that was copied into.
     */
    public SslOptions copyInto(SslOptions other) {
        other.setKeyStoreLocation(getKeyStoreLocation());
        other.setKeyStorePassword(getKeyStorePassword());
        other.setTrustStoreLocation(getTrustStoreLocation());
        other.setTrustStorePassword(getTrustStorePassword());
        other.setKeyStoreType(getKeyStoreType());
        other.setTrustStoreType(getTrustStoreType());
        other.setEnabledCipherSuites(getEnabledCipherSuites());
        other.setDisabledCipherSuites(getDisabledCipherSuites());
        other.setEnabledProtocols(getEnabledProtocols());
        other.setDisabledProtocols(getDisabledProtocols());
        other.setTrustAll(isTrustAll());
        other.setVerifyHost(isVerifyHost());
        other.setKeyAlias(getKeyAlias());
        other.setContextProtocol(getContextProtocol());
        other.setDefaultSslPort(getDefaultSslPort());
        other.setSslContextOverride(getSslContextOverride());
        other.setAllowNativeSSL(isAllowNativeSSL());

        return other;
    }
}
