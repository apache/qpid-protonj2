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
package org.apache.qpid.protonj2.client.transport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.security.UnrecoverableKeyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.junit.jupiter.api.Test;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.OpenSslEngine;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

/**
 * Tests for the TransportSupport class.
 */
@SuppressWarnings("deprecation")
public class SslSupportTest extends ImperativeClientTestCase {

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
        assertFalse(engineProtocols.contains("SSLv3"), "SSLv3 should not be enabled by default");
        assertFalse(engineProtocols.contains("SSLv2Hello"), "SSLv2Hello should not be enabled by default");
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
        assertFalse(engineProtocols.contains("SSLv3"), "SSLv3 should not be enabled by default");

        // TODO - Netty is currently unable to disable OpenSSL SSLv2Hello so we are stuck with it for now.
        // assertFalse(engineProtocols.contains("SSLv2Hello"), "SSLv2Hello should not be enabled by default");
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
        options.contextProtocol(contextProtocol);

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
        options.contextProtocol(contextProtocol);

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);

        // TODO There is no means currently of getting the protocol from the netty SslContext.
        // assertEquals(contextProtocol, context.getProtocol());
    }

    @Test
    public void testCreateSslContextNoKeyStorePasswordJDK() throws Exception {
        SslOptions options = createJksSslOptions();
        options.keyStorePassword(null);

        assertThrows(UnrecoverableKeyException.class, () -> {
            SslSupport.createJdkSslContext(options);
        });
    }

    @Test
    public void testCreateSslContextNoKeyStorePasswordOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJksSslOptions();
        options.keyStorePassword(null);

        assertThrows(UnrecoverableKeyException.class, () -> {
            SslSupport.createOpenSslContext(options);
        });
    }

    @Test
    public void testCreateSslContextWrongKeyStorePasswordJDK() throws Exception {
        SslOptions options = createJksSslOptions();
        options.keyStorePassword("wrong");

        assertThrows(IOException.class, () -> {
            SslSupport.createJdkSslContext(options);
        });
    }

    @Test
    public void testCreateSslContextWrongKeyStorePasswordOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJksSslOptions();
        options.keyStorePassword("wrong");

        assertThrows(IOException.class, () -> {
            SslSupport.createOpenSslContext(options);
        });
    }

    @Test
    public void testCreateSslContextBadPathToKeyStoreJDK() throws Exception {
        SslOptions options = createJksSslOptions();
        options.keyStoreLocation(CLIENT_JKS_KEYSTORE + ".bad");

        assertThrows(IOException.class, () -> {
            SslSupport.createJdkSslContext(options);
        });
    }

    @Test
    public void testCreateSslContextBadPathToKeyStoreOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJksSslOptions();
        options.keyStoreLocation(CLIENT_JKS_KEYSTORE + ".bad");

        assertThrows(IOException.class, () -> {
            SslSupport.createOpenSslContext(options);
        });
    }

    @Test
    public void testCreateSslContextWrongTrustStorePasswordJDK() throws Exception {
        SslOptions options = createJksSslOptions();
        options.trustStorePassword("wrong");

        assertThrows(IOException.class, () -> {
            SslSupport.createJdkSslContext(options);
        });
    }

    @Test
    public void testCreateSslContextWrongTrustStorePasswordOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJksSslOptions();
        options.trustStorePassword("wrong");

        assertThrows(IOException.class, () -> {
            SslSupport.createOpenSslContext(options);
        });
    }

    @Test
    public void testCreateSslContextBadPathToTrustStoreJDK() throws Exception {
        SslOptions options = createJksSslOptions();
        options.trustStoreLocation(CLIENT_JKS_TRUSTSTORE + ".bad");

        assertThrows(IOException.class, () -> {
            SslSupport.createJdkSslContext(options);
        });
    }

    @Test
    public void testCreateSslContextBadPathToTrustStoreOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createJksSslOptions();
        options.trustStoreLocation(CLIENT_JKS_TRUSTSTORE + ".bad");

        assertThrows(IOException.class, () -> {
            SslSupport.createOpenSslContext(options);
        });
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

    @Test
    public void testCreateSslContextIncorrectStoreTypeJDK() throws Exception {
        SslOptions options = createPkcs12SslOptions();
        options.storeType(KEYSTORE_JCEKS_TYPE);

        assertThrows(IOException.class, () -> {
            SslSupport.createJdkSslContext(options);
        });
    }

    @Test
    public void testCreateSslContextIncorrectStoreTypeOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = createPkcs12SslOptions();
        options.storeType(KEYSTORE_JCEKS_TYPE);

        assertThrows(IOException.class, () -> {
            SslSupport.createOpenSslContext(options);
        });
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

        assertArrayEquals(ENABLED_PROTOCOLS, engine.getEnabledProtocols(), "Enabled protocols not as expected");
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

        assertArrayEquals(ENABLED_OPENSSL_PROTOCOLS, engine.getEnabledProtocols(), "Enabled protocols not as expected");
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

        assertArrayEquals(ENABLED_PROTOCOLS, engine.getEnabledProtocols(), "Enabled protocols not as expected");
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

        assertArrayEquals(ENABLED_OPENSSL_PROTOCOLS, engine.getEnabledProtocols(), "Enabled protocols not as expected");
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitDisabledProtocolsJDK() throws Exception {
        // Discover the default enabled protocols
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] protocols = directEngine.getEnabledProtocols();
        assertTrue(protocols.length > 0, "There were no initial protocols to choose from!");

        // Pull out one to disable specifically
        String[] disabledProtocol = new String[] { protocols[protocols.length - 1] };
        String[] trimmedProtocols = Arrays.copyOf(protocols, protocols.length - 1);
        options.disabledProtocols(disabledProtocol);
        SSLContext context = SslSupport.createJdkSslContext(options);
        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals(trimmedProtocols, engine.getEnabledProtocols(), "Enabled protocols not as expected");
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitDisabledProtocolsOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Discover the default enabled protocols
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createOpenSSLEngineDirectly(options);
        String[] protocols = directEngine.getEnabledProtocols();
        assertTrue(protocols.length > 0, "There were no initial protocols to choose from!");

        // Pull out one to disable specifically
        String[] disabledProtocol = new String[] { protocols[protocols.length - 1] };
        String[] trimmedProtocols = Arrays.copyOf(protocols, protocols.length - 1);
        options.disabledProtocols(disabledProtocol);
        SslContext context = SslSupport.createOpenSslContext(options);
        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals(trimmedProtocols, engine.getEnabledProtocols(), "Enabled protocols not as expected");
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledAndDisabledProtocolsJDK() throws Exception {
        // Discover the default enabled protocols
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] protocols = directEngine.getEnabledProtocols();
        assertTrue(protocols.length > 1, "There were no initial protocols to choose from!");

        // Pull out two to enable, and one to disable specifically
        String protocol1 = protocols[0];
        String protocol2 = protocols[1];
        String[] enabledProtocols = new String[] { protocol1, protocol2 };
        String[] disabledProtocol = new String[] { protocol1 };
        String[] remainingProtocols = new String[] { protocol2 };
        options.enabledProtocols(enabledProtocols);
        options.disabledProtocols(disabledProtocol);
        SSLContext context = SslSupport.createJdkSslContext(options);
        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);

        // verify the option took effect, that the disabled protocols were removed from the enabled list.
        assertNotNull(engine);
        assertArrayEquals(remainingProtocols, engine.getEnabledProtocols(), "Enabled protocols not as expected");
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledAndDisabledProtocolsOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Discover the default enabled protocols
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createOpenSSLEngineDirectly(options);
        String[] protocols = directEngine.getEnabledProtocols();
        assertTrue(protocols.length > 1, "There were no initial protocols to choose from!");

        // Pull out two to enable, and one to disable specifically
        String protocol1 = protocols[0];
        String protocol2 = protocols[1];
        String[] enabledProtocols = new String[] { protocol1, protocol2 };
        String[] disabledProtocol = new String[] { protocol1 };
        String[] remainingProtocols = new String[] { protocol2 };
        options.enabledProtocols(enabledProtocols);
        options.disabledProtocols(disabledProtocol);
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
        assertEquals(remainingProtocolsList.size(), engine.getEnabledProtocols().length, "Enabled protocols not as expected");
        assertTrue(remainingProtocolsList.containsAll(Arrays.asList(engine.getEnabledProtocols())), "Enabled protocols not as expected");
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledCiphersJDK() throws Exception {
        // Discover the default enabled ciphers
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue(ciphers.length > 0, "There were no initial ciphers to choose from!");

        // Pull out one to enable specifically
        String cipher = ciphers[0];
        String[] enabledCipher = new String[] { cipher };
        options.enabledCipherSuites(enabledCipher);
        SSLContext context = SslSupport.createJdkSslContext(options);
        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals(enabledCipher, engine.getEnabledCipherSuites(), "Enabled ciphers not as expected");
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledCiphersOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Discover the default enabled ciphers
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createOpenSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue(ciphers.length > 0, "There were no initial ciphers to choose from!");

        // Pull out one to enable specifically
        String cipher = ciphers[0];
        String[] enabledCipher = new String[] { cipher };
        options.enabledCipherSuites(enabledCipher);
        SslContext context = SslSupport.createOpenSslContext(options);
        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals(enabledCipher, engine.getEnabledCipherSuites(), "Enabled ciphers not as expected");
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitDisabledCiphersJDK() throws Exception {
        // Discover the default enabled ciphers
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue(ciphers.length > 0, "There were no initial ciphers to choose from!");

        // Pull out one to disable specifically
        String[] disabledCipher = new String[] { ciphers[ciphers.length - 1] };
        String[] trimmedCiphers = Arrays.copyOf(ciphers, ciphers.length - 1);
        options.disabledCipherSuites(disabledCipher);
        SSLContext context = SslSupport.createJdkSslContext(options);
        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals(trimmedCiphers, engine.getEnabledCipherSuites(), "Enabled ciphers not as expected");
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitDisabledCiphersOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Discover the default enabled ciphers
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createOpenSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue(ciphers.length > 0, "There were no initial ciphers to choose from!");

        // Pull out one to disable specifically
        String[] disabledCipher = new String[] { ciphers[ciphers.length - 1] };
        String[] trimmedCiphers = Arrays.copyOf(ciphers, ciphers.length - 1);
        options.disabledCipherSuites(disabledCipher);
        SslContext context = SslSupport.createOpenSslContext(options);
        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);

        // verify the option took effect
        assertNotNull(engine);
        assertArrayEquals(trimmedCiphers, engine.getEnabledCipherSuites(), "Enabled ciphers not as expected");
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledAndDisabledCiphersJDK() throws Exception {
        // Discover the default enabled ciphers
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue(ciphers.length > 1, "There werent enough initial ciphers to choose from!");

        // Pull out two to enable, and one to disable specifically
        String cipher1 = ciphers[0];
        String cipher2 = ciphers[1];
        String[] enabledCiphers = new String[] { cipher1, cipher2 };
        String[] disabledCipher = new String[] { cipher1 };
        String[] remainingCipher = new String[] { cipher2 };
        options.enabledCipherSuites(enabledCiphers);
        options.disabledCipherSuites(disabledCipher);
        SSLContext context = SslSupport.createJdkSslContext(options);
        SSLEngine engine = SslSupport.createJdkSslEngine(null, -1, context, options);

        // verify the option took effect, that the disabled ciphers were removed from the enabled list.
        assertNotNull(engine);
        assertArrayEquals(remainingCipher, engine.getEnabledCipherSuites(), "Enabled ciphers not as expected");
    }

    @Test
    public void testCreateSslEngineFromJksStoreWithExplicitEnabledAndDisabledCiphersOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        // Discover the default enabled ciphers
        SslOptions options = createJksSslOptions();
        SSLEngine directEngine = createOpenSSLEngineDirectly(options);
        String[] ciphers = directEngine.getEnabledCipherSuites();
        assertTrue(ciphers.length > 1, "There werent enough initial ciphers to choose from!");

        // Pull out two to enable, and one to disable specifically
        String cipher1 = ciphers[0];
        String cipher2 = ciphers[1];
        String[] enabledCiphers = new String[] { cipher1, cipher2 };
        String[] disabledCipher = new String[] { cipher1 };
        String[] remainingCipher = new String[] { cipher2 };
        options.enabledCipherSuites(enabledCiphers);
        options.disabledCipherSuites(disabledCipher);
        SslContext context = SslSupport.createOpenSslContext(options);
        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);

        // verify the option took effect, that the disabled ciphers were removed from the enabled list.
        assertNotNull(engine);
        assertArrayEquals(remainingCipher, engine.getEnabledCipherSuites(), "Enabled ciphers not as expected");
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

        assertArrayEquals(ENABLED_PROTOCOLS, engine.getEnabledProtocols(), "Enabled protocols not as expected");
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

        assertArrayEquals(ENABLED_OPENSSL_PROTOCOLS, engine.getEnabledProtocols(), "Enabled protocols not as expected");
    }

    @Test
    public void testCreateSslEngineWithVerifyHostJDK() throws Exception {
        SslOptions options = createJksSslOptions();
        options.verifyHost(true);

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
        options.verifyHost(true);

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);
        assertNotNull(engine);

        assertEquals("HTTPS", engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @Test
    public void testCreateSslEngineWithoutVerifyHostJDK() throws Exception {
        SslOptions options = createJksSslOptions();
        options.verifyHost(false);

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
        options.verifyHost(false);

        SslContext context = SslSupport.createOpenSslContext(options);
        assertNotNull(context);

        SSLEngine engine = SslSupport.createOpenSslEngine(PooledByteBufAllocator.DEFAULT, null, -1, context, options);
        assertNotNull(engine);

        assertNull(engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @Test
    public void testCreateSslContextWithKeyAliasWhichDoesntExist() throws Exception {
        SslOptions options = createJksSslOptions();
        options.keyAlias(ALIAS_DOES_NOT_EXIST);

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
        options.keyAlias(ALIAS_CA_CERT);

        try {
            SslSupport.createJdkSslContext(options);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            // Expected
        }
    }

    @Test
    public void testIsOpenSSLPossible() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = new SslOptions();
        options.allowNativeSSL(false);
        assertFalse(SslSupport.isOpenSSLPossible(options));

        options.allowNativeSSL(true);
        assertTrue(SslSupport.isOpenSSLPossible(options));
    }

    @Test
    public void testIsOpenSSLPossibleWhenHostNameVerificationConfigured() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());
        assumeTrue(OpenSsl.supportsHostnameValidation());

        SslOptions options = new SslOptions();
        options.allowNativeSSL(true);

        options.verifyHost(false);
        assertTrue(SslSupport.isOpenSSLPossible(options));

        options.verifyHost(true);
        assertTrue(SslSupport.isOpenSSLPossible(options));
    }

    @Test
    public void testIsOpenSSLPossibleWhenKeyAliasIsSpecified() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());
        assumeTrue(OpenSsl.supportsHostnameValidation());

        SslOptions options = new SslOptions();
        options.allowNativeSSL(true);
        options.keyAlias("alias");

        assertFalse(SslSupport.isOpenSSLPossible(options));
    }

    @Test
    public void testCreateSslHandlerJDK() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = new SslOptions();
        options.sslEnabled(true);
        options.allowNativeSSL(false);

        SslHandler handler = SslSupport.createSslHandler(null, null, -1, options);
        assertNotNull(handler);
        assertFalse(handler.engine() instanceof OpenSslEngine);
    }

    @Test
    public void testCreateSslHandlerOpenSSL() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = new SslOptions();
        options.allowNativeSSL(true);

        SslHandler handler = SslSupport.createSslHandler(PooledByteBufAllocator.DEFAULT, null, -1, options);
        assertNotNull(handler);
        assertTrue(handler.engine() instanceof OpenSslEngine);
    }

    @Test
    public void testCreateOpenSSLEngineFailsWhenAllocatorMissing() throws Exception {
        assumeTrue(OpenSsl.isAvailable());
        assumeTrue(OpenSsl.supportsKeyManagerFactory());

        SslOptions options = new SslOptions();
        options.allowNativeSSL(true);

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

        options.sslEnabled(true);
        options.keyStoreLocation(CLIENT_JKS_KEYSTORE);
        options.trustStoreLocation(CLIENT_JKS_TRUSTSTORE);
        options.storeType(KEYSTORE_JKS_TYPE);
        options.keyStorePassword(PASSWORD);
        options.trustStorePassword(PASSWORD);
        if (enabledProtocols != null) {
            options.enabledProtocols(enabledProtocols);
        }

        return options;
    }

    private SslOptions createJceksSslOptions() {
        return createJceksSslOptions(null);
    }

    private SslOptions createJceksSslOptions(String[] enabledProtocols) {
        SslOptions options = new SslOptions();

        options.sslEnabled(true);
        options.keyStoreLocation(CLIENT_JCEKS_KEYSTORE);
        options.trustStoreLocation(CLIENT_JCEKS_TRUSTSTORE);
        options.storeType(KEYSTORE_JCEKS_TYPE);
        options.keyStorePassword(PASSWORD);
        options.trustStorePassword(PASSWORD);
        if (enabledProtocols != null) {
            options.enabledProtocols(enabledProtocols);
        }

        return options;
    }

    private SslOptions createPkcs12SslOptions() {
        return createPkcs12SslOptions(null);
    }

    private SslOptions createPkcs12SslOptions(String[] enabledProtocols) {
        SslOptions options = new SslOptions();

        options.keyStoreLocation(CLIENT_PKCS12_KEYSTORE);
        options.trustStoreLocation(CLIENT_PKCS12_TRUSTSTORE);
        options.storeType(KEYSTORE_PKCS12_TYPE);
        options.keyStorePassword(PASSWORD);
        options.trustStorePassword(PASSWORD);
        if (enabledProtocols != null) {
            options.enabledProtocols(enabledProtocols);
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
