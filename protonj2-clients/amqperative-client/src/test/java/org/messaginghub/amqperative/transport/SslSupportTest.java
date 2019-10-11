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
package org.messaginghub.amqperative.transport;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.security.UnrecoverableKeyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.junit.Test;
import org.messaginghub.amqperative.SslOptions;
import org.messaginghub.amqperative.test.AMQPerativeTestCase;
import org.messaginghub.amqperative.transport.SslSupport;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.OpenSslEngine;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

/**
 * Tests for the TransportSupport class.
 */
@SuppressWarnings("deprecation")
public class SslSupportTest extends AMQPerativeTestCase {

    public static final String PASSWORD = "password";

    public static final String HOSTNAME = "localhost";

    public static final String BROKER_JKS_KEYSTORE = "src/test/resources/broker-jks.keystore";
    public static final String BROKER_JKS_TRUSTSTORE = "src/test/resources/broker-jks.truststore";
    public static final String CLIENT_JKS_KEYSTORE = "src/test/resources/client-jks.keystore";
    public static final String CLIENT_JKS_TRUSTSTORE = "src/test/resources/client-jks.truststore";

    public static final String BROKER_JCEKS_KEYSTORE = "src/test/resources/broker-jceks.keystore";
    public static final String BROKER_JCEKS_TRUSTSTORE = "src/test/resources/broker-jceks.truststore";
    public static final String CLIENT_JCEKS_KEYSTORE = "src/test/resources/client-jceks.keystore";
    public static final String CLIENT_JCEKS_TRUSTSTORE = "src/test/resources/client-jceks.truststore";

    public static final String BROKER_PKCS12_KEYSTORE = "src/test/resources/broker-pkcs12.keystore";
    public static final String BROKER_PKCS12_TRUSTSTORE = "src/test/resources/broker-pkcs12.truststore";
    public static final String CLIENT_PKCS12_KEYSTORE = "src/test/resources/client-pkcs12.keystore";
    public static final String CLIENT_PKCS12_TRUSTSTORE = "src/test/resources/client-pkcs12.truststore";

    public static final String KEYSTORE_JKS_TYPE = "jks";
    public static final String KEYSTORE_JCEKS_TYPE = "jceks";
    public static final String KEYSTORE_PKCS12_TYPE = "pkcs12";

    public static final String[] ENABLED_PROTOCOLS = new String[] { "TLSv1" };

    // Currently the OpenSSL implementation cannot disable SSLv2Hello
    public static final String[] ENABLED_OPENSSL_PROTOCOLS = new String[] { "SSLv2Hello", "TLSv1" };

    private static final String ALIAS_DOES_NOT_EXIST = "alias.does.not.exist";
    private static final String ALIAS_CA_CERT = "ca";

    @Test
    public void testLegacySslProtocolsDisabledByDefaultJDK() throws Exception {
        SslOptions options = createJksSslOptions(null);

        SSLContext context = SslSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse("SSLv3 should not be enabled by default", engineProtocols.contains("SSLv3"));
        assertFalse("SSLv2Hello should not be enabled by default", engineProtocols.contains("SSLv2Hello"));
    }

    @Test
    public void testLegacySslProtocolsDisabledByDefaultOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJksSslOptions(null);

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse("SSLv3 should not be enabled by default", engineProtocols.contains("SSLv3"));

        // TODO - Netty is currently unable to disable OpenSSL SSLv2Hello so we are stuck with it for now.
        // assertFalse("SSLv2Hello should not be enabled by default", engineProtocols.contains("SSLv2Hello"));
    }

    @Test
    public void testCreateSslContextJksStoreJDK() throws Exception {
        SslOptions options = createJksSslOptions();

        SSLContext context = SslSupport.createJdkSslContext(options);
        assertNotNull(context);

        assertEquals("TLS", context.getProtocol());
    }

    @Test
    public void testCreateSslContextJksStoreOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJksSslOptions();

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);

        // TODO There is no means currently of getting the protocol from the netty SslContext.
        // assertEquals("TLS", context.getProtocol());
    }

    @Test
    public void testCreateSslContextJksStoreWithConfiguredContextProtocolJDK() throws Exception {
        SslOptions options = createJksSslOptions();
        String contextProtocol = "TLSv1.2";
        options.setContextProtocol(contextProtocol);

        SSLContext context = SslSupport.createJdkSslContext(options);
        assertNotNull(context);

        assertEquals(contextProtocol, context.getProtocol());
    }

    @Test
    public void testCreateSslContextJksStoreWithConfiguredContextProtocolOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJksSslOptions();
        String contextProtocol = "TLSv1.2";
        options.setContextProtocol(contextProtocol);

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);

        // TODO There is no means currently of getting the protocol from the netty SslContext.
        // assertEquals(contextProtocol, context.getProtocol());
    }

    @Test(expected = UnrecoverableKeyException.class)
    public void testCreateSslContextNoKeyStorePasswordJDK() throws Exception {
        SslOptions options = createJksSslOptions();
        options.setKeyStorePassword(null);
        SslSupport.createJdkSslContext(options);
    }

    @Test(expected = UnrecoverableKeyException.class)
    public void testCreateSslContextNoKeyStorePasswordOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJksSslOptions();
        options.setKeyStorePassword(null);
        SslSupport.createOpenSslContext(options);
    }

    @Test(expected = IOException.class)
    public void testCreateSslContextWrongKeyStorePasswordJDK() throws Exception {
        SslOptions options = createJksSslOptions();
        options.setKeyStorePassword("wrong");
        SslSupport.createJdkSslContext(options);
    }

    @Test(expected = IOException.class)
    public void testCreateSslContextWrongKeyStorePasswordOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJksSslOptions();
        options.setKeyStorePassword("wrong");
        SslSupport.createOpenSslContext(options);
    }

    @Test(expected = IOException.class)
    public void testCreateSslContextBadPathToKeyStoreJDK() throws Exception {
        SslOptions options = createJksSslOptions();
        options.setKeyStoreLocation(CLIENT_JKS_KEYSTORE + ".bad");
        SslSupport.createJdkSslContext(options);
    }

    @Test(expected = IOException.class)
    public void testCreateSslContextBadPathToKeyStoreOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJksSslOptions();
        options.setKeyStoreLocation(CLIENT_JKS_KEYSTORE + ".bad");
        SslSupport.createOpenSslContext(options);
    }

    @Test(expected = IOException.class)
    public void testCreateSslContextWrongTrustStorePasswordJDK() throws Exception {
        SslOptions options = createJksSslOptions();
        options.setTrustStorePassword("wrong");
        SslSupport.createJdkSslContext(options);
    }

    @Test(expected = IOException.class)
    public void testCreateSslContextWrongTrustStorePasswordOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJksSslOptions();
        options.setTrustStorePassword("wrong");
        SslSupport.createOpenSslContext(options);
    }

    @Test(expected = IOException.class)
    public void testCreateSslContextBadPathToTrustStoreJDK() throws Exception {
        SslOptions options = createJksSslOptions();
        options.setTrustStoreLocation(CLIENT_JKS_TRUSTSTORE + ".bad");
        SslSupport.createJdkSslContext(options);
    }

    @Test(expected = IOException.class)
    public void testCreateSslContextBadPathToTrustStoreOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJksSslOptions();
        options.setTrustStoreLocation(CLIENT_JKS_TRUSTSTORE + ".bad");
        SslSupport.createOpenSslContext(options);
    }

    @Test
    public void testCreateSslContextJceksStoreJDK() throws Exception {
        SslOptions options = createJceksSslOptions();

        SSLContext context = SslSupport.createJdkSslContext(options);
        assertNotNull(context);

        assertEquals("TLS", context.getProtocol());
    }

    @Test
    public void testCreateSslContextJceksStoreOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJceksSslOptions();

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);
        assertTrue(context.isClient());
    }

    @Test
    public void testCreateSslContextPkcs12StoreJDK() throws Exception {
        SslOptions options = createPkcs12SslOptions();

        SSLContext context = SslSupport.createJdkSslContext(options);
        assertNotNull(context);

        assertEquals("TLS", context.getProtocol());
    }

    @Test
    public void testCreateSslContextPkcs12StoreOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createPkcs12SslOptions();

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);
        assertTrue(context.isClient());
    }

    @Test(expected = IOException.class)
    public void testCreateSslContextIncorrectStoreTypeJDK() throws Exception {
        SslOptions options = createPkcs12SslOptions();
        options.setStoreType(KEYSTORE_JCEKS_TYPE);
        SslSupport.createJdkSslContext(options);
    }

    @Test(expected = IOException.class)
    public void testCreateSslContextIncorrectStoreTypeOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createPkcs12SslOptions();
        options.setStoreType(KEYSTORE_JCEKS_TYPE);
        SslSupport.createOpenSslContext(options);
    }

    @Test
    public void testCreateSslEngineFromPkcs12StoreJDK() throws Exception {
        SslOptions options = createPkcs12SslOptions();

        SSLContext context = SslSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.isEmpty());
    }

    @Test
    public void testCreateSslEngineFromPkcs12StoreOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createPkcs12SslOptions();

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.isEmpty());
    }

    @Test
    public void testCreateSslEngineFromPkcs12StoreWithExplicitEnabledProtocolsJDK() throws Exception {
        SslOptions options = createPkcs12SslOptions(ENABLED_PROTOCOLS);

        SSLContext context = SslSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);
        assertNotNull(engine);

        assertArrayEquals("Enabled protocols not as expected", ENABLED_PROTOCOLS, engine.getEnabledProtocols());
    }

    @Test
    public void testCreateSslEngineFromPkcs12StoreWithExplicitEnabledProtocolsOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createPkcs12SslOptions(ENABLED_PROTOCOLS);

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);
        assertNotNull(engine);

        assertArrayEquals("Enabled protocols not as expected", ENABLED_OPENSSL_PROTOCOLS, engine.getEnabledProtocols());
    }

    @Test
    public void testCreateSslEngineFromJksStoreJDK() throws Exception {
        SslOptions options = createJksSslOptions();

        SSLContext context = SslSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.isEmpty());
    }

    @Test
    public void testCreateSslEngineFromJksStoreOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJksSslOptions();

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.isEmpty());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledProtocolsJDK() throws Exception {
        SslOptions options = createJksSslOptions(ENABLED_PROTOCOLS);

        SSLContext context = SslSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);
        assertNotNull(engine);

        assertArrayEquals("Enabled protocols not as expected", ENABLED_PROTOCOLS, engine.getEnabledProtocols());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledProtocolsOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJksSslOptions(ENABLED_PROTOCOLS);

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);
        assertNotNull(engine);

        assertArrayEquals("Enabled protocols not as expected", ENABLED_OPENSSL_PROTOCOLS, engine.getEnabledProtocols());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitDisabledProtocolsJDK() throws Exception {
        // Discover the default enabled protocols
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] protocols = directEngine.getEnabledProtocols();
        assertTrue("There were no initial protocols to choose from!", protocols.length > 0);

        // Pull out one to disable specifically
        String[] disabledProtocol = new String[] { protocols[protocols.length - 1] };
        String[] trimmedProtocols = Arrays.copyOf(protocols, protocols.length - 1);
        options.setDisabledProtocols(disabledProtocol);
        SSLContext context = SslSupport.createJdkSslContext(options);
        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals("Enabled protocols not as expected", trimmedProtocols, engine.getEnabledProtocols());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitDisabledProtocolsOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Discover the default enabled protocols
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createOpenSSLEngineDirectly(options);
        String[] protocols = directEngine.getEnabledProtocols();
        assertTrue("There were no initial protocols to choose from!", protocols.length > 0);

        // Pull out one to disable specifically
        String[] disabledProtocol = new String[] { protocols[protocols.length - 1] };
        String[] trimmedProtocols = Arrays.copyOf(protocols, protocols.length - 1);
        options.setDisabledProtocols(disabledProtocol);
        SslContext context = SslSupport.createOpenSslContext(options);
        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals("Enabled protocols not as expected", trimmedProtocols, engine.getEnabledProtocols());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledAndDisabledProtocolsJDK() throws Exception {
        // Discover the default enabled protocols
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] protocols = directEngine.getEnabledProtocols();
        assertTrue("There were no initial protocols to choose from!", protocols.length > 1);

        // Pull out two to enable, and one to disable specifically
        String protocol1 = protocols[0];
        String protocol2 = protocols[1];
        String[] enabledProtocols = new String[] { protocol1, protocol2 };
        String[] disabledProtocol = new String[] { protocol1 };
        String[] remainingProtocols = new String[] { protocol2 };
        options.setEnabledProtocols(enabledProtocols);
        options.setDisabledProtocols(disabledProtocol);
        SSLContext context = SslSupport.createJdkSslContext(options);
        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);

        // verify the option took effect, that the disabled protocols were removed from the enabled list.
        assertNotNull(engine);
        assertArrayEquals("Enabled protocols not as expected", remainingProtocols, engine.getEnabledProtocols());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledAndDisabledProtocolsOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Discover the default enabled protocols
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createOpenSSLEngineDirectly(options);
        String[] protocols = directEngine.getEnabledProtocols();
        assertTrue("There were no initial protocols to choose from!", protocols.length > 1);

        // Pull out two to enable, and one to disable specifically
        String protocol1 = protocols[0];
        String protocol2 = protocols[1];
        String[] enabledProtocols = new String[] { protocol1, protocol2 };
        String[] disabledProtocol = new String[] { protocol1 };
        String[] remainingProtocols = new String[] { protocol2 };
        options.setEnabledProtocols(enabledProtocols);
        options.setDisabledProtocols(disabledProtocol);
        SslContext context = SslSupport.createOpenSslContext(options);
        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);

        // Because Netty cannot currently disable SSLv2Hello in OpenSSL we need to account for it popping up.
        ArrayList<String> remainingProtocolsList = new ArrayList<>(Arrays.asList(remainingProtocols));
        if (!remainingProtocolsList.contains("SSLv2Hello")) {
            remainingProtocolsList.add(0, "SSLv2Hello");
        }

        remainingProtocols = remainingProtocolsList.toArray(new String[remainingProtocolsList.size()]);

        // verify the option took effect, that the disabled protocols were removed from the enabled list.
        assertNotNull(engine);
        assertEquals("Enabled protocols not as expected", remainingProtocolsList.size(), engine.getEnabledProtocols().length);
        assertTrue("Enabled protocols not as expected", remainingProtocolsList.containsAll(Arrays.asList(engine.getEnabledProtocols())));
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledCiphersJDK() throws Exception {
        // Discover the default enabled ciphers
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue("There were no initial ciphers to choose from!", ciphers.length > 0);

        // Pull out one to enable specifically
        String cipher = ciphers[0];
        String[] enabledCipher = new String[] { cipher };
        options.setEnabledCipherSuites(enabledCipher);
        SSLContext context = SslSupport.createJdkSslContext(options);
        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals("Enabled ciphers not as expected", enabledCipher, engine.getEnabledCipherSuites());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledCiphersOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Discover the default enabled ciphers
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createOpenSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue("There were no initial ciphers to choose from!", ciphers.length > 0);

        // Pull out one to enable specifically
        String cipher = ciphers[0];
        String[] enabledCipher = new String[] { cipher };
        options.setEnabledCipherSuites(enabledCipher);
        SslContext context = SslSupport.createOpenSslContext(options);
        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals("Enabled ciphers not as expected", enabledCipher, engine.getEnabledCipherSuites());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitDisabledCiphersJDK() throws Exception {
        // Discover the default enabled ciphers
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue("There were no initial ciphers to choose from!", ciphers.length > 0);

        // Pull out one to disable specifically
        String[] disabledCipher = new String[] { ciphers[ciphers.length - 1] };
        String[] trimmedCiphers = Arrays.copyOf(ciphers, ciphers.length - 1);
        options.setDisabledCipherSuites(disabledCipher);
        SSLContext context = SslSupport.createJdkSslContext(options);
        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals("Enabled ciphers not as expected", trimmedCiphers, engine.getEnabledCipherSuites());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitDisabledCiphersOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Discover the default enabled ciphers
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createOpenSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue("There were no initial ciphers to choose from!", ciphers.length > 0);

        // Pull out one to disable specifically
        String[] disabledCipher = new String[] { ciphers[ciphers.length - 1] };
        String[] trimmedCiphers = Arrays.copyOf(ciphers, ciphers.length - 1);
        options.setDisabledCipherSuites(disabledCipher);
        SslContext context = SslSupport.createOpenSslContext(options);
        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals("Enabled ciphers not as expected", trimmedCiphers, engine.getEnabledCipherSuites());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledAndDisabledCiphersJDK() throws Exception {
        // Discover the default enabled ciphers
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue("There werent enough initial ciphers to choose from!", ciphers.length > 1);

        // Pull out two to enable, and one to disable specifically
        String cipher1 = ciphers[0];
        String cipher2 = ciphers[1];
        String[] enabledCiphers = new String[] { cipher1, cipher2 };
        String[] disabledCipher = new String[] { cipher1 };
        String[] remainingCipher = new String[] { cipher2 };
        options.setEnabledCipherSuites(enabledCiphers);
        options.setDisabledCipherSuites(disabledCipher);
        SSLContext context = SslSupport.createJdkSslContext(options);
        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);

        // verify the option took effect, that the disabled ciphers were removed from the enabled list.
        assertNotNull(engine);
        assertArrayEquals("Enabled ciphers not as expected", remainingCipher, engine.getEnabledCipherSuites());
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledAndDisabledCiphersOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Discover the default enabled ciphers
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createOpenSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue("There werent enough initial ciphers to choose from!", ciphers.length > 1);

        // Pull out two to enable, and one to disable specifically
        String cipher1 = ciphers[0];
        String cipher2 = ciphers[1];
        String[] enabledCiphers = new String[] { cipher1, cipher2 };
        String[] disabledCipher = new String[] { cipher1 };
        String[] remainingCipher = new String[] { cipher2 };
        options.setEnabledCipherSuites(enabledCiphers);
        options.setDisabledCipherSuites(disabledCipher);
        SslContext context = SslSupport.createOpenSslContext(options);
        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);

        // verify the option took effect, that the disabled ciphers were removed from the enabled list.
        assertNotNull(engine);
        assertArrayEquals("Enabled ciphers not as expected", remainingCipher, engine.getEnabledCipherSuites());
    }

    @Test
    public void testCreateSslEngineFromJceksStoreJDK() throws Exception {
        SslOptions options = createJceksSslOptions();

        SSLContext context = SslSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.isEmpty());
    }

    @Test
    public void testCreateSslEngineFromJceksStoreOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJceksSslOptions();

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);
        assertNotNull(engine);

        List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
        assertFalse(engineProtocols.isEmpty());
    }

    @Test
    public void testCreateSslEngineFromJceksStoreWithExplicitEnabledProtocolsJDK() throws Exception {
        SslOptions options = createJceksSslOptions(ENABLED_PROTOCOLS);

        SSLContext context = SslSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);
        assertNotNull(engine);

        assertArrayEquals("Enabled protocols not as expected", ENABLED_PROTOCOLS, engine.getEnabledProtocols());
    }

    @Test
    public void testCreateSslEngineFromJceksStoreWithExplicitEnabledProtocolsOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Try and disable all but the one we really want but for now expect that this one plus SSLv2Hello
        // is going to come back until the netty code can successfully disable them all.
        SslOptions options = createJceksSslOptions(ENABLED_PROTOCOLS);

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);
        assertNotNull(engine);

        assertArrayEquals("Enabled protocols not as expected", ENABLED_OPENSSL_PROTOCOLS, engine.getEnabledProtocols());
    }

    @Test
    public void testCreateSslEngineWithVerifyHostJDK() throws Exception {
        SslOptions options = createJksSslOptions();
        options.setVerifyHost(true);

        SSLContext context = SslSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);
        assertNotNull(engine);

        assertEquals("HTTPS", engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @Test
    public void testCreateSslEngineWithVerifyHostOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());
        assumeTrue(OpenSsl.supportsHostnameValidation());

        SslOptions options = createJksSslOptions();
        options.setVerifyHost(true);

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);
        assertNotNull(engine);

        assertEquals("HTTPS", engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @Test
    public void testCreateSslEngineWithoutVerifyHostJDK() throws Exception {
        SslOptions options = createJksSslOptions();
        options.setVerifyHost(false);

        SSLContext context = SslSupport.createJdkSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);
        assertNotNull(engine);

        assertNull(engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @Test
    public void testCreateSslEngineWithoutVerifyHostOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());
        assumeTrue(OpenSsl.supportsHostnameValidation());

        SslOptions options = createJksSslOptions();
        options.setVerifyHost(false);

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);
        assertNotNull(engine);

        assertNull(engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @Test
    public void testCreateSslContextWithKeyAliasWhichDoesntExist() throws Exception {
        SslOptions options = createJksSslOptions();
        options.setKeyAlias(ALIAS_DOES_NOT_EXIST);

        try {
            SslSupport.createJdkSslContext(options);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }

    @Test
    public void testCreateSslContextWithKeyAliasWhichRepresentsNonKeyEntry() throws Exception {
        SslOptions options = createJksSslOptions();
        options.setKeyAlias(ALIAS_CA_CERT);

        try {
            SslSupport.createJdkSslContext(options);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }

    @Test(timeout = 100000)
    public void testIsOpenSSLPossible() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = new SslOptions();
        options.setAllowNativeSSL(false);
        assertFalse(SslSupport.isOpenSSLPossible(options));

        options.setAllowNativeSSL(true);
        assertTrue(SslSupport.isOpenSSLPossible(options));
    }

    @Test(timeout = 100000)
    public void testIsOpenSSLPossibleWhenHostNameVerificationConfigured() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());
        assumeTrue(OpenSsl.supportsHostnameValidation());

        SslOptions options = new SslOptions();
        options.setAllowNativeSSL(true);

        options.setVerifyHost(false);
        assertTrue(SslSupport.isOpenSSLPossible(options));

        options.setVerifyHost(true);
        assertTrue(SslSupport.isOpenSSLPossible(options));
    }

    @Test(timeout = 100000)
    public void testIsOpenSSLPossibleWhenKeyAliasIsSpecified() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());
        assumeTrue(OpenSsl.supportsHostnameValidation());

        SslOptions options = new SslOptions();
        options.setAllowNativeSSL(true);
        options.setKeyAlias("alias");

        assertFalse(SslSupport.isOpenSSLPossible(options));
    }

    @Test(timeout = 100000)
    public void testCreateSslHandlerJDK() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = new SslOptions();
        options.setSSLEnabled(true);
        options.setAllowNativeSSL(false);

        SslHandler handler = SslSupport.createSslHandler(null, null, -1, options);
        assertNotNull(handler);
        assertFalse(handler.engine() instanceof OpenSslEngine);
    }

    @Test(timeout = 100000)
    public void testCreateSslHandlerOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = new SslOptions();
        options.setAllowNativeSSL(true);

        SslHandler handler = SslSupport.createSslHandler(PooledByteBufAllocator.DEFAULT, null, -1, options);
        assertNotNull(handler);
        assertTrue(handler.engine() instanceof OpenSslEngine);
    }

    @Test(timeout = 100000)
    public void testCreateOpenSSLEngineFailsWhenAllocatorMissing() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = new SslOptions();
        options.setAllowNativeSSL(true);

        SslContext context = SslSupport.createOpenSslContext(options);
        try {
            SslSupport.createOpenSslEngine(null, null, -1, context, options);
            fail("Should throw IllegalArgumentException for null allocator.");
        } catch (IllegalArgumentException iae) {}
    }

    private SslOptions createJksSslOptions() {
        return createJksSslOptions(null);
    }

    private SslOptions createJksSslOptions(String[] enabledProtocols) {
        SslOptions options = new SslOptions();

        options.setSSLEnabled(true);
        options.setKeyStoreLocation(CLIENT_JKS_KEYSTORE);
        options.setTrustStoreLocation(CLIENT_JKS_TRUSTSTORE);
        options.setStoreType(KEYSTORE_JKS_TYPE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStorePassword(PASSWORD);
        if (enabledProtocols != null) {
            options.setEnabledProtocols(enabledProtocols);
        }

        return options;
    }

    private SslOptions createJceksSslOptions() {
        return createJceksSslOptions(null);
    }

    private SslOptions createJceksSslOptions(String[] enabledProtocols) {
        SslOptions options = new SslOptions();

        options.setSSLEnabled(true);
        options.setKeyStoreLocation(CLIENT_JCEKS_KEYSTORE);
        options.setTrustStoreLocation(CLIENT_JCEKS_TRUSTSTORE);
        options.setStoreType(KEYSTORE_JCEKS_TYPE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStorePassword(PASSWORD);
        if (enabledProtocols != null) {
            options.setEnabledProtocols(enabledProtocols);
        }

        return options;
    }

    private SslOptions createPkcs12SslOptions() {
        return createPkcs12SslOptions(null);
    }

    private SslOptions createPkcs12SslOptions(String[] enabledProtocols) {
        SslOptions options = new SslOptions();

        options.setKeyStoreLocation(CLIENT_PKCS12_KEYSTORE);
        options.setTrustStoreLocation(CLIENT_PKCS12_TRUSTSTORE);
        options.setStoreType(KEYSTORE_PKCS12_TYPE);
        options.setKeyStorePassword(PASSWORD);
        options.setTrustStorePassword(PASSWORD);
        if (enabledProtocols != null) {
            options.setEnabledProtocols(enabledProtocols);
        }

        return options;
    }

    private SSLEngine createSSLEngineDirectly(SslOptions options) throws Exception {
        SSLContext context = SslSupport.createJdkSslContext(options);
        SSLEngine engine = context.createSSLEngine();
        return engine;
    }

    private SSLEngine createOpenSSLEngineDirectly(SslOptions options) throws Exception {
        SslContext context = SslSupport.createOpenSslContext(options);
        SSLEngine engine = context.newEngine(PooledByteBufAllocator.DEFAULT);
        return engine;
    }
}
