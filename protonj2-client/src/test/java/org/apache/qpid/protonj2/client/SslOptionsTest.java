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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.net.ssl.SSLContext;

import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;

/**
 * Tests for the {@link SslOptions} class
 */
public class SslOptionsTest extends ImperativeClientTestCase {

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

    @Override
    @AfterEach
    public void tearDown(TestInfo testInfo) throws Exception {
        super.tearDown(testInfo);
    }

    @Override
    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
    }

    @Test
    public void testCreate() {
        SslOptions options = new SslOptions();

        assertFalse(options.sslEnabled());

        assertEquals(SslOptions.DEFAULT_TRUST_ALL, options.trustAll());
        assertEquals(SslOptions.DEFAULT_STORE_TYPE, options.keyStoreType());
        assertEquals(SslOptions.DEFAULT_STORE_TYPE, options.trustStoreType());

        assertEquals(SslOptions.DEFAULT_CONTEXT_PROTOCOL, options.contextProtocol());
        assertNull(options.enabledProtocols());
        assertArrayEquals(SslOptions.DEFAULT_DISABLED_PROTOCOLS.toArray(new String[0]), options.disabledProtocols());
        assertNull(options.enabledCipherSuites());
        assertNull(options.disabledCipherSuites());

        assertNull(options.keyStoreLocation());
        assertNull(options.keyStorePassword());
        assertNull(options.trustStoreLocation());
        assertNull(options.trustStorePassword());
        assertNull(options.keyAlias());
        assertNull(options.sslContextOverride());
    }

    @Test
    public void testClone() {
        SslOptions original = createNonDefaultOptions();
        SslOptions options = original.clone();

        assertNotSame(original, options);
        assertTrue(options.sslEnabled());
        assertEquals(TEST_DEFAULT_SSL_PORT, options.defaultSslPort());
        assertEquals(CLIENT_KEYSTORE, options.keyStoreLocation());
        assertEquals(PASSWORD, options.keyStorePassword());
        assertEquals(CLIENT_TRUSTSTORE, options.trustStoreLocation());
        assertEquals(PASSWORD, options.trustStorePassword());
        assertEquals(KEYSTORE_TYPE, options.keyStoreType());
        assertEquals(KEYSTORE_TYPE, options.trustStoreType());
        assertEquals(KEY_ALIAS, options.keyAlias());
        assertEquals(CONTEXT_PROTOCOL, options.contextProtocol());
        assertEquals(SSL_CONTEXT, options.sslContextOverride());
        assertArrayEquals(ENABLED_PROTOCOLS,options.enabledProtocols());
        assertArrayEquals(DISABLED_PROTOCOLS,options.disabledProtocols());
        assertArrayEquals(ENABLED_CIPHERS,options.enabledCipherSuites());
        assertArrayEquals(DISABLED_CIPHERS,options.disabledCipherSuites());
    }

    @Test
    public void testCreateAndConfigure() {
        SslOptions options = createNonDefaultOptions();

        assertEquals(CLIENT_KEYSTORE, options.keyStoreLocation());
        assertEquals(PASSWORD, options.keyStorePassword());
        assertEquals(CLIENT_TRUSTSTORE, options.trustStoreLocation());
        assertEquals(PASSWORD, options.trustStorePassword());
        assertEquals(KEYSTORE_TYPE, options.keyStoreType());
        assertEquals(KEYSTORE_TYPE, options.trustStoreType());
        assertEquals(KEY_ALIAS, options.keyAlias());
        assertEquals(CONTEXT_PROTOCOL, options.contextProtocol());
        assertEquals(SSL_CONTEXT, options.sslContextOverride());
        assertArrayEquals(ENABLED_PROTOCOLS,options.enabledProtocols());
        assertArrayEquals(DISABLED_PROTOCOLS,options.disabledProtocols());
        assertArrayEquals(ENABLED_CIPHERS,options.enabledCipherSuites());
        assertArrayEquals(DISABLED_CIPHERS,options.disabledCipherSuites());
    }

    @Test
    public void testSslSystemPropertiesInfluenceDefaults() {
        String keystore = "keystore";
        String keystorePass = "keystorePass";
        String truststore = "truststore";
        String truststorePass = "truststorePass";

        setSslSystemPropertiesForCurrentTest(keystore, keystorePass, truststore, truststorePass);

        SslOptions options1 = new SslOptions();

        assertEquals(keystore, options1.keyStoreLocation());
        assertEquals(keystorePass, options1.keyStorePassword());
        assertEquals(truststore, options1.trustStoreLocation());
        assertEquals(truststorePass, options1.trustStorePassword());

        keystore +="2";
        keystorePass +="2";
        truststore +="2";
        truststorePass +="2";

        setSslSystemPropertiesForCurrentTest(keystore, keystorePass, truststore, truststorePass);

        SslOptions options2 = new SslOptions();

        assertEquals(keystore, options2.keyStoreLocation());
        assertEquals(keystorePass, options2.keyStorePassword());
        assertEquals(truststore, options2.trustStoreLocation());
        assertEquals(truststorePass, options2.trustStorePassword());

        assertNotEquals(options1.keyStoreLocation(), options2.keyStoreLocation());
        assertNotEquals(options1.keyStorePassword(), options2.keyStorePassword());
        assertNotEquals(options1.trustStoreLocation(), options2.trustStoreLocation());
        assertNotEquals(options1.trustStorePassword(), options2.trustStorePassword());
    }

    private void setSslSystemPropertiesForCurrentTest(String keystore, String keystorePassword, String truststore, String truststorePassword) {
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE, keystore);
        setTestSystemProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, keystorePassword);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE, truststore);
        setTestSystemProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, truststorePassword);
    }

    private SslOptions createNonDefaultOptions() {
        SslOptions options = new SslOptions();

        options.sslEnabled(true);
        options.defaultSslPort(TEST_DEFAULT_SSL_PORT);
        options.allowNativeSSL(TEST_ALLOW_NATIVE_SSL);
        options.keyStoreLocation(CLIENT_KEYSTORE);
        options.keyStorePassword(PASSWORD);
        options.trustStoreLocation(CLIENT_TRUSTSTORE);
        options.trustStorePassword(PASSWORD);
        options.trustAll(TRUST_ALL);
        options.verifyHost(VERIFY_HOST);
        options.keyAlias(KEY_ALIAS);
        options.contextProtocol(CONTEXT_PROTOCOL);
        options.sslContextOverride(SSL_CONTEXT);
        options.enabledProtocols(ENABLED_PROTOCOLS);
        options.enabledCipherSuites(ENABLED_CIPHERS);
        options.disabledProtocols(DISABLED_PROTOCOLS);
        options.disabledCipherSuites(DISABLED_CIPHERS);

        return options;
    }
}
