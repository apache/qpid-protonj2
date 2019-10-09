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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import javax.net.ssl.SSLContext;

import org.junit.Test;
import org.messaginghub.amqperative.test.AMQPerativeTestCase;
import org.mockito.Mockito;

/**
 * Tests for the {@link SslOptions} class
 */
public class SslOptionsTest extends AMQPerativeTestCase {

    private static final String PASSWORD = "password";
    private static final String CLIENT_KEYSTORE = "src/test/resources/client-jks.keystore";
    private static final String CLIENT_TRUSTSTORE = "src/test/resources/client-jks.truststore";
    private static final String KEYSTORE_TYPE = "jks";
    private static final String KEY_ALIAS = "myTestAlias";
    private static final String CONTEXT_PROTOCOL = "TLSv1.1";
    private static final boolean TRUST_ALL = true;
    private static final boolean VERIFY_HOST = true;

    private static final int TEST_DEFAULT_SSL_PORT = 5681;
    private static final boolean TEST_ALLOW_NATIVE_SSL = false;

    private static final String[] ENABLED_PROTOCOLS = new String[] {"TLSv1.2"};
    private static final String[] DISABLED_PROTOCOLS = new String[] {"SSLv3", "TLSv1.2"};
    private static final String[] ENABLED_CIPHERS = new String[] {"CIPHER_A", "CIPHER_B"};
    private static final String[] DISABLED_CIPHERS = new String[] {"CIPHER_C"};

    private static final SSLContext SSL_CONTEXT = Mockito.mock(SSLContext.class);

    private static final String JAVAX_NET_SSL_KEY_STORE = "javax.net.ssl.keyStore";
    private static final String JAVAX_NET_SSL_KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
    private static final String JAVAX_NET_SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    private static final String JAVAX_NET_SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";

    @Test
    public void testCreate() {
        SslOptions options = new SslOptions();

        assertEquals(SslOptions.DEFAULT_TRUST_ALL, options.isTrustAll());
        assertEquals(SslOptions.DEFAULT_STORE_TYPE, options.getKeyStoreType());
        assertEquals(SslOptions.DEFAULT_STORE_TYPE, options.getTrustStoreType());

        assertEquals(SslOptions.DEFAULT_CONTEXT_PROTOCOL, options.getContextProtocol());
        assertNull(options.getEnabledProtocols());
        assertArrayEquals(SslOptions.DEFAULT_DISABLED_PROTOCOLS.toArray(new String[0]), options.getDisabledProtocols());
        assertNull(options.getEnabledCipherSuites());
        assertNull(options.getDisabledCipherSuites());

        assertNull(options.getKeyStoreLocation());
        assertNull(options.getKeyStorePassword());
        assertNull(options.getTrustStoreLocation());
        assertNull(options.getTrustStorePassword());
        assertNull(options.getKeyAlias());
        assertNull(options.getSslContextOverride());
    }

    @Test
    public void testClone() {
        SslOptions options = createNonDefaultOptions().clone();

        assertEquals(TEST_DEFAULT_SSL_PORT, options.getDefaultSslPort());
        assertEquals(CLIENT_KEYSTORE, options.getKeyStoreLocation());
        assertEquals(PASSWORD, options.getKeyStorePassword());
        assertEquals(CLIENT_TRUSTSTORE, options.getTrustStoreLocation());
        assertEquals(PASSWORD, options.getTrustStorePassword());
        assertEquals(KEYSTORE_TYPE, options.getKeyStoreType());
        assertEquals(KEYSTORE_TYPE, options.getTrustStoreType());
        assertEquals(KEY_ALIAS, options.getKeyAlias());
        assertEquals(CONTEXT_PROTOCOL, options.getContextProtocol());
        assertEquals(SSL_CONTEXT, options.getSslContextOverride());
        assertArrayEquals(ENABLED_PROTOCOLS,options.getEnabledProtocols());
        assertArrayEquals(DISABLED_PROTOCOLS,options.getDisabledProtocols());
        assertArrayEquals(ENABLED_CIPHERS,options.getEnabledCipherSuites());
        assertArrayEquals(DISABLED_CIPHERS,options.getDisabledCipherSuites());
    }

    @Test
    public void testCreateAndConfigure() {
        SslOptions options = createNonDefaultOptions();

        assertEquals(CLIENT_KEYSTORE, options.getKeyStoreLocation());
        assertEquals(PASSWORD, options.getKeyStorePassword());
        assertEquals(CLIENT_TRUSTSTORE, options.getTrustStoreLocation());
        assertEquals(PASSWORD, options.getTrustStorePassword());
        assertEquals(KEYSTORE_TYPE, options.getKeyStoreType());
        assertEquals(KEYSTORE_TYPE, options.getTrustStoreType());
        assertEquals(KEY_ALIAS, options.getKeyAlias());
        assertEquals(CONTEXT_PROTOCOL, options.getContextProtocol());
        assertEquals(SSL_CONTEXT, options.getSslContextOverride());
        assertArrayEquals(ENABLED_PROTOCOLS,options.getEnabledProtocols());
        assertArrayEquals(DISABLED_PROTOCOLS,options.getDisabledProtocols());
        assertArrayEquals(ENABLED_CIPHERS,options.getEnabledCipherSuites());
        assertArrayEquals(DISABLED_CIPHERS,options.getDisabledCipherSuites());
    }

    @Test
    public void testSslSystemPropertiesInfluenceDefaults() {
        String keystore = "keystore";
        String keystorePass = "keystorePass";
        String truststore = "truststore";
        String truststorePass = "truststorePass";

        setSslSystemPropertiesForCurrentTest(keystore, keystorePass, truststore, truststorePass);

        SslOptions options1 = new SslOptions();

        assertEquals(keystore, options1.getKeyStoreLocation());
        assertEquals(keystorePass, options1.getKeyStorePassword());
        assertEquals(truststore, options1.getTrustStoreLocation());
        assertEquals(truststorePass, options1.getTrustStorePassword());

        keystore +="2";
        keystorePass +="2";
        truststore +="2";
        truststorePass +="2";

        setSslSystemPropertiesForCurrentTest(keystore, keystorePass, truststore, truststorePass);

        SslOptions options2 = new SslOptions();

        assertEquals(keystore, options2.getKeyStoreLocation());
        assertEquals(keystorePass, options2.getKeyStorePassword());
        assertEquals(truststore, options2.getTrustStoreLocation());
        assertEquals(truststorePass, options2.getTrustStorePassword());

        assertNotEquals(options1.getKeyStoreLocation(), options2.getKeyStoreLocation());
        assertNotEquals(options1.getKeyStorePassword(), options2.getKeyStorePassword());
        assertNotEquals(options1.getTrustStoreLocation(), options2.getTrustStoreLocation());
        assertNotEquals(options1.getTrustStorePassword(), options2.getTrustStorePassword());
    }

    private void setSslSystemPropertiesForCurrentTest(String keystore, String keystorePassword, String truststore, String truststorePassword) {
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE, keystore);
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, keystorePassword);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE, truststore);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, truststorePassword);
    }

    private SslOptions createNonDefaultOptions() {
        SslOptions options = new SslOptions();

        options.setDefaultSslPort(TEST_DEFAULT_SSL_PORT);
        options.setAllowNativeSSL(TEST_ALLOW_NATIVE_SSL);
        options.setKeyStoreLocation(CLIENT_KEYSTORE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStoreLocation(CLIENT_TRUSTSTORE);
        options.setTrustStorePassword(PASSWORD);
        options.setTrustAll(TRUST_ALL);
        options.setVerifyHost(VERIFY_HOST);
        options.setKeyAlias(KEY_ALIAS);
        options.setContextProtocol(CONTEXT_PROTOCOL);
        options.setSslContextOverride(SSL_CONTEXT);
        options.setEnabledProtocols(ENABLED_PROTOCOLS);
        options.setEnabledCipherSuites(ENABLED_CIPHERS);
        options.setDisabledProtocols(DISABLED_PROTOCOLS);
        options.setDisabledCipherSuites(DISABLED_CIPHERS);

        return options;
    }
}
