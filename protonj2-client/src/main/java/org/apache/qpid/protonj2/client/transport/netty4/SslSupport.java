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
package org.apache.qpid.protonj2.client.transport.netty4;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.transport.X509AliasKeyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.OpenSslX509KeyManagerFactory;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

/**
 * Static class that provides various utility methods used by Transport implementations.
 */
public final class SslSupport {

    private static final Logger LOG = LoggerFactory.getLogger(SslSupport.class);

    public static final String FROM_CLASSPATH_PREFIX = "classpath:";
    public static final String FROM_FILE_PREFIX = "file:";
    public static final String FROM_FILE_URL_PREFIX = "file://";

    /**
     * Determines if Netty OpenSSL support is available and applicable based on the configuration
     * in the given TransportOptions instance.
     *
     * @param options
     * 		  The configuration of the Transport being created.
     *
     * @return true if OpenSSL support is available and usable given the requested configuration.
     */
    public static boolean isOpenSSLPossible(SslOptions options) {
        boolean result = false;

        if (options.allowNativeSSL()) {
            if (!OpenSsl.isAvailable()) {
                LOG.debug("OpenSSL could not be enabled because a suitable implementation could not be found.", OpenSsl.unavailabilityCause());
            } else if (options.sslContextOverride() != null) {
                LOG.debug("OpenSSL could not be enabled due to user SSLContext being supplied.");
            } else if (!OpenSsl.supportsKeyManagerFactory()) {
                LOG.debug("OpenSSL could not be enabled because the version provided does not allow a KeyManagerFactory to be used.");
            } else if (options.keyAlias() != null) {
                LOG.debug("OpenSSL could not be enabled because a keyAlias is set and that feature is not supported for OpenSSL.");
            } else {
                LOG.debug("OpenSSL Enabled: Version {} of OpenSSL will be used", OpenSsl.versionString());
                result = true;
            }
        }

        return result;
    }

    /**
     * Creates a Netty SslHandler instance for use in Transports that require
     * an SSL encoder / decoder.
     *
     * If the given options contain an SSLContext override, this will be used directly
     * when creating the handler. If they do not, an SSLContext will first be created
     * using the other option values.
     *
     * @param allocator
     *		  The Netty Buffer Allocator to use when Netty resources need to be created.
     * @param host
     *        the host name or IP address that this transport connects to.
     * @param port
     * 		  the port on the given host that this transport connects to.
     * @param options
     *        The SSL options object to build the SslHandler instance from.
     *
     * @return a new SslHandler that is configured from the given options.
     *
     * @throws Exception if an error occurs while creating the SslHandler instance.
     */
    public static SslHandler createSslHandler(ByteBufAllocator allocator, String host, int port, SslOptions options) throws Exception {
        final SSLEngine sslEngine;

        if (isOpenSSLPossible(options)) {
            SslContext sslContext = createOpenSslContext(options);
            sslEngine = createOpenSslEngine(allocator, host, port, sslContext, options);
        } else {
            SSLContext sslContext = options.sslContextOverride();
            if (sslContext == null) {
                sslContext = createJdkSslContext(options);
            }

            sslEngine = createJdkSslEngine(host, port, sslContext, options);
        }

        return new SslHandler(sslEngine);
    }

    //----- JDK SSL Support Methods ------------------------------------------//

    /**
     * Create a new SSLContext using the options specific in the given TransportOptions
     * instance.
     *
     * @param options
     *        the configured options used to create the SSLContext.
     *
     * @return a new SSLContext instance.
     *
     * @throws Exception if an error occurs while creating the context.
     */
    public static SSLContext createJdkSslContext(SslOptions options) throws Exception {
        try {
            String contextProtocol = options.contextProtocol();
            LOG.trace("Getting SSLContext instance using protocol: {}", contextProtocol);

            SSLContext context = SSLContext.getInstance(contextProtocol);

            KeyManager[] keyMgrs = loadKeyManagers(options);
            TrustManager[] trustManagers = loadTrustManagers(options);

            context.init(keyMgrs, trustManagers, new SecureRandom());
            return context;
        } catch (Exception e) {
            LOG.error("Failed to create SSLContext: {}", e, e);
            throw e;
        }
    }

    /**
     * Create a new JDK SSLEngine instance in client mode from the given SSLContext and
     * TransportOptions instances.
     *
     * @param host
     *        the host name or IP address that this transport connects to.
     * @param port
     * 		  the port on the given host that this transport connects to.
     * @param context
     *        the SSLContext to use when creating the engine.
     * @param options
     *        the TransportOptions to use to configure the new SSLEngine.
     *
     * @return a new SSLEngine instance in client mode.
     *
     * @throws Exception if an error occurs while creating the new SSLEngine.
     */
    public static SSLEngine createJdkSslEngine(String host, int port, SSLContext context, SslOptions options) throws Exception {
        SSLEngine engine = null;
        if (host == null || host.isEmpty()) {
            engine = context.createSSLEngine();
        } else {
            engine = context.createSSLEngine(host, port);
        }

        engine.setEnabledProtocols(buildEnabledProtocols(engine, options));
        engine.setEnabledCipherSuites(buildEnabledCipherSuites(engine, options));
        engine.setUseClientMode(true);
        engine.setSSLParameters(createSSLParameters(engine, options));

        return engine;
    }

    //----- OpenSSL Support Methods ------------------------------------------//

    /**
     * Create a new Netty SslContext using the options specific in the given TransportOptions
     * instance.
     *
     * @param options
     *        the configured options used to create the SslContext.
     *
     * @return a new SslContext instance.
     *
     * @throws Exception if an error occurs while creating the context.
     */
    public static SslContext createOpenSslContext(SslOptions options) throws Exception {
        try {
            String contextProtocol = options.contextProtocol();
            LOG.trace("Getting SslContext instance using protocol: {}", contextProtocol);

            KeyManagerFactory keyManagerFactory = loadKeyManagerFactory(options, SslProvider.OPENSSL);
            TrustManagerFactory trustManagerFactory = loadTrustManagerFactory(options);
            SslContextBuilder builder = SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL);

            // TODO - There is oddly no way in Netty right now to get the set of supported protocols
            //        when creating the SslContext or really even when creating the SSLEngine.  Seems
            //        like an oversight, for now we call it with TLSv1.2 so it looks like we did something.
            if (options.contextProtocol().equals(SslOptions.DEFAULT_CONTEXT_PROTOCOL)) {
                builder.protocols("TLSv1.2");
            } else {
                builder.protocols(options.contextProtocol());
            }
            builder.keyManager(keyManagerFactory);
            builder.trustManager(trustManagerFactory);

            return builder.build();
        } catch (Exception e) {
            LOG.error("Failed to create SslContext: {}", e, e);
            throw e;
        }
    }

    /**
     * Create a new OpenSSL SSLEngine instance in client mode from the given SSLContext and
     * TransportOptions instances.
     *
     * @param allocator
     *		  the Netty ByteBufAllocator to use to create the OpenSSL engine
     * @param host
     *        the host name or IP address that this transport connects to.
     * @param port
     * 		  the port on the given host that this transport connects to.
     * @param context
     *        the Netty SslContext to use when creating the engine.
     * @param options
     *        the TransportOptions to use to configure the new SSLEngine.
     *
     * @return a new Netty managed SSLEngine instance in client mode.
     *
     * @throws Exception if an error occurs while creating the new SSLEngine.
     */
    public static SSLEngine createOpenSslEngine(ByteBufAllocator allocator, String host, int port, SslContext context, SslOptions options) throws Exception {
        SSLEngine engine = null;

        if (allocator == null) {
            throw new IllegalArgumentException("OpenSSL engine requires a valid ByteBufAllocator to operate");
        }

        if (host == null || host.isEmpty()) {
            engine = context.newEngine(allocator);
        } else {
            engine = context.newEngine(allocator, host, port);
        }

        engine.setEnabledProtocols(buildEnabledProtocols(engine, options));
        engine.setEnabledCipherSuites(buildEnabledCipherSuites(engine, options));
        engine.setUseClientMode(true);
        engine.setSSLParameters(createSSLParameters(engine, options));

        return engine;
    }

    //----- Internal support methods -----------------------------------------//

    private static SSLParameters createSSLParameters(SSLEngine engine, SslOptions options) {
        final SSLParameters sslParameters = engine.getSSLParameters();

        if (options.verifyHost()) {
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
        } else {
            sslParameters.setEndpointIdentificationAlgorithm(null);
        }

        return sslParameters;
    }

    private static String[] buildEnabledProtocols(SSLEngine engine, SslOptions options) {
        List<String> enabledProtocols = new ArrayList<String>();

        if (options.enabledProtocols() != null) {
            List<String> configuredProtocols = Arrays.asList(options.enabledProtocols());
            LOG.trace("Configured protocols from transport options: {}", configuredProtocols);
            enabledProtocols.addAll(configuredProtocols);
        } else {
            List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
            LOG.trace("Default protocols from the SSLEngine: {}", engineProtocols);
            enabledProtocols.addAll(engineProtocols);
        }

        String[] disabledProtocols = options.disabledProtocols();
        if (disabledProtocols != null) {
            List<String> disabled = Arrays.asList(disabledProtocols);
            LOG.trace("Disabled protocols: {}", disabled);
            enabledProtocols.removeAll(disabled);
        }

        LOG.trace("Enabled protocols: {}", enabledProtocols);

        return enabledProtocols.toArray(new String[0]);
    }

    private static String[] buildEnabledCipherSuites(SSLEngine engine, SslOptions options) {
        List<String> enabledCipherSuites = new ArrayList<String>();

        if (options.enabledCipherSuites() != null) {
            List<String> configuredCipherSuites = Arrays.asList(options.enabledCipherSuites());
            LOG.trace("Configured cipher suites from transport options: {}", configuredCipherSuites);
            enabledCipherSuites.addAll(configuredCipherSuites);
        } else {
            List<String> engineCipherSuites = Arrays.asList(engine.getEnabledCipherSuites());
            LOG.trace("Default cipher suites from the SSLEngine: {}", engineCipherSuites);
            enabledCipherSuites.addAll(engineCipherSuites);
        }

        String[] disabledCipherSuites = options.disabledCipherSuites();
        if (disabledCipherSuites != null) {
            List<String> disabled = Arrays.asList(disabledCipherSuites);
            LOG.trace("Disabled cipher suites: {}", disabled);
            enabledCipherSuites.removeAll(disabled);
        }

        LOG.trace("Enabled cipher suites: {}", enabledCipherSuites);

        return enabledCipherSuites.toArray(new String[0]);
    }

    private static TrustManager[] loadTrustManagers(SslOptions options) throws Exception {
        TrustManagerFactory factory = loadTrustManagerFactory(options);
        if (factory != null) {
            return factory.getTrustManagers();
        } else {
            return null;
        }
    }

    private static TrustManagerFactory loadTrustManagerFactory(SslOptions options) throws Exception {
        if (options.trustAll()) {
            return InsecureTrustManagerFactory.INSTANCE;
        }

        if (options.trustStoreLocation() == null) {
            return null;
        }

        TrustManagerFactory fact = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        String storeLocation = options.trustStoreLocation();
        String storePassword = options.trustStorePassword();
        String storeType = options.trustStoreType();

        LOG.trace("Attempt to load TrustStore from location {} of type {}", storeLocation, storeType);

        KeyStore trustStore = loadStore(storeLocation, storePassword, storeType);
        fact.init(trustStore);

        return fact;
    }

    private static KeyManager[] loadKeyManagers(SslOptions options) throws Exception {
        if (options.keyStoreLocation() == null) {
            return null;
        }

        KeyManagerFactory fact = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

        String storeLocation = options.keyStoreLocation();
        String storePassword = options.keyStorePassword();
        String storeType = options.keyStoreType();
        String alias = options.keyAlias();

        LOG.trace("Attempt to load KeyStore from location {} of type {}", storeLocation, storeType);

        KeyStore keyStore = loadStore(storeLocation, storePassword, storeType);
        fact.init(keyStore, storePassword != null ? storePassword.toCharArray() : null);

        if (alias == null) {
            return fact.getKeyManagers();
        } else {
            validateAlias(keyStore, alias);
            return wrapKeyManagers(alias, fact.getKeyManagers());
        }
    }

    private static KeyManagerFactory loadKeyManagerFactory(SslOptions options, SslProvider provider) throws Exception {
        if (options.keyStoreLocation() == null) {
            return null;
        }

        final KeyManagerFactory factory;
        if (provider.equals(SslProvider.JDK)) {
            factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        } else {
            factory = new OpenSslX509KeyManagerFactory();
        }

        String storeLocation = options.keyStoreLocation();
        String storePassword = options.keyStorePassword();
        String storeType = options.keyStoreType();

        LOG.trace("Attempt to load KeyStore from location {} of type {}", storeLocation, storeType);

        KeyStore keyStore = loadStore(storeLocation, storePassword, storeType);
        factory.init(keyStore, storePassword != null ? storePassword.toCharArray() : null);

        return factory;
    }

    private static KeyManager[] wrapKeyManagers(String alias, KeyManager[] origKeyManagers) {
        KeyManager[] keyManagers = new KeyManager[origKeyManagers.length];
        for (int i = 0; i < origKeyManagers.length; i++) {
            KeyManager km = origKeyManagers[i];
            if (km instanceof X509ExtendedKeyManager) {
                km = new X509AliasKeyManager(alias, (X509ExtendedKeyManager) km);
            }

            keyManagers[i] = km;
        }

        return keyManagers;
    }

    private static void validateAlias(KeyStore store, String alias) throws IllegalArgumentException, KeyStoreException {
        if (!store.containsAlias(alias)) {
            throw new IllegalArgumentException("The alias '" + alias + "' doesn't exist in the key store");
        }

        if (!store.isKeyEntry(alias)) {
            throw new IllegalArgumentException("The alias '" + alias + "' in the keystore doesn't represent a key entry");
        }
    }

    private static KeyStore loadStore(String storePath, final String password, String storeType) throws Exception {
        final KeyStore store = KeyStore.getInstance(storeType);

        try (InputStream in = openStoreAtLocation(storePath)) {
            store.load(in, password != null ? password.toCharArray() : null);
        } catch (Exception ex) {
            LOG.trace("Caught Error loading store: {}", ex.getMessage(), ex);
            throw ex;
        }

        return store;
    }

    private static InputStream openStoreAtLocation(final String storePath) throws IOException {
        final InputStream stream;

        if (storePath.startsWith(FROM_CLASSPATH_PREFIX)) {
            stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(storePath.substring(FROM_CLASSPATH_PREFIX.length()));
        } else if (storePath.startsWith(FROM_FILE_URL_PREFIX)) {
            stream = new FileInputStream(storePath.substring(FROM_FILE_URL_PREFIX.length()));
        } else if (storePath.startsWith(FROM_FILE_PREFIX)) {
            stream = new FileInputStream(storePath.substring(FROM_FILE_PREFIX.length()));
        } else {
            stream = new FileInputStream(storePath);
        }

        if (stream == null) {
            throw new IOException("Could no locate KeyStore at location: " + storePath);
        }

        return stream;
    }
}
