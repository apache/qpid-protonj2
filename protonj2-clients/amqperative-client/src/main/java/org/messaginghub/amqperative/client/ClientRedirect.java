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
package org.messaginghub.amqperative.client;

import static org.messaginghub.amqperative.client.ClientConstants.NETWORK_HOST;
import static org.messaginghub.amqperative.client.ClientConstants.OPEN_HOSTNAME;
import static org.messaginghub.amqperative.client.ClientConstants.PATH;
import static org.messaginghub.amqperative.client.ClientConstants.PORT;
import static org.messaginghub.amqperative.client.ClientConstants.SCHEME;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.messaginghub.amqperative.util.PropertyUtil;
import org.messaginghub.amqperative.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the AMQP Redirect Map
 */
public class ClientRedirect {

    private static final Logger LOG = LoggerFactory.getLogger(ClientRedirect.class);

    private final Map<Symbol, Object> redirect;
    private final ClientConnection connection;

    public ClientRedirect(Map<Symbol, Object> redirect, ClientConnection connection) {
        this.redirect = redirect;
        this.connection = connection;

        if (connection == null) {
            throw new IllegalArgumentException("A Client Connection instance is required");
        }

        URI remoteURI = connection.getRemoteURI();
        if (remoteURI == null || remoteURI.getScheme() == null || remoteURI.getScheme().isEmpty()) {
            throw new IllegalArgumentException("The provider instance must provide a valid scheme");
        }
    }

    public ClientRedirect validate() throws Exception {
        String networkHost = (String) redirect.get(NETWORK_HOST);
        if (networkHost == null || networkHost.isEmpty()) {
            throw new IOException("Redirection information not set, missing network host.");
        }

        final int networkPort;
        try {
            networkPort = Integer.parseInt(redirect.get(PORT).toString());
        } catch (Exception ex) {
            throw new IOException("Redirection information contained invalid port.");
        }

        LOG.trace("Redirect issued host and port as follows: {}:{}", networkHost, networkPort);

        String sourceScheme = connection.getRemoteURI().getScheme();
        String scheme = (String) redirect.get(SCHEME);
        if (scheme != null && !scheme.isEmpty() && !scheme.equals(sourceScheme)) {

            // TODO - Implement means of determining the URI scheme and other
            //        data such as allow insecure redirects.

//            // Attempt to located a provider using normal scheme (amqp, amqps, etc...)
//            ProviderFactory factory = null;
//            try {
//                factory = ProviderFactory.findProviderFactory(scheme);
//            } catch (Throwable error) {
//                LOG.trace("Couldn't find AMQP prefixed Provider using scheme: {}", scheme);
//            }
//
//            if (factory == null) {
//                // Attempt to located a transport level redirect (ws, wss, etc...)
//                try {
//                    factory = findProviderFactoryByTransportScheme(scheme);
//                } catch (Throwable error) {
//                    LOG.trace("Couldn't find Provider using transport scheme: {}", scheme);
//                }
//            }
//
//            if (factory == null || !(factory instanceof AmqpProviderFactory)) {
//                throw new IOException("Redirect contained an unknown provider scheme: " + scheme);
//            }
//
//            LOG.trace("Found provider: {} for redirect: {}", factory.getName(), scheme);
//
//            AmqpProviderFactory amqpFactory = (AmqpProviderFactory) factory;
//            String transportType = amqpFactory.getTransportScheme();
//
//            if (transportType == null || transportType.isEmpty()) {
//                throw new IOException("Redirect contained an unknown provider scheme: " + scheme);
//            }
//
//            TransportFactory transportFactory = TransportFactory.findTransportFactory(transportType);
//            if (transportFactory == null) {
//                throw new IOException("Redirect contained an unknown provider scheme: " + scheme);
//            }
//
//            ConnectionOptions options = connection.getOptions();
//
//            // Check for insecure redirect and whether it is allowed.
//            if (options.getTransport().isSecure() && !transportFactory.isSecure() && !options.isAllowNonSecureRedirects()) {
//                throw new IOException("Attempt to redirect to an insecure connection type: " + transportType);
//            }
//
//            // Update the redirect information with the resolved target scheme used to create
//            // the provider for the redirection.
//            redirect.put(SCHEME, amqpFactory.getProviderScheme());
        }

        // Check it actually converts to URI since we require it do so later
        toURI();

        return this;
    }

    /**
     * @return the redirection map that backs this object
     */
    public Map<Symbol, Object> getRedirectMap() {
        return redirect;
    }

    /**
     * @return the host name of the container being redirected to.
     */
    public String getHostname() {
        return (String) redirect.get(OPEN_HOSTNAME);
    }

    /**
     * @return the DNS host name or IP address of the peer this connection is being redirected to.
     */
    public String getNetworkHost() {
        return (String) redirect.get(NETWORK_HOST);
    }

    /**
     * @return the port number on the peer this connection is being redirected to.
     */
    public int getPort() {
        return Integer.parseInt(redirect.get(PORT).toString());
    }

    /**
     * @return the scheme that the remote indicated the redirect connection should use.
     */
    public String getScheme() {
        String scheme = (String) redirect.get(SCHEME);
        if (scheme == null || scheme.isEmpty()) {
            scheme = connection.getRemoteURI().getScheme();
        }

        return scheme;
    }

    /**
     * @return the path that the remote indicated should be path of the redirect URI.
     */
    public String getPath() {
        return (String) redirect.get(PATH);
    }

    /**
     * Construct a URI from the redirection information available.
     *
     * @return a URI that matches the redirection information provided.
     *
     * @throws Exception if an error occurs construct a URI from the redirection information.
     */
    public URI toURI() throws Exception {
        Map<String, String> queryOptions = PropertyUtil.parseQuery(connection.getRemoteURI());

        URI result = new URI(getScheme(), null, getNetworkHost(), getPort(), getPath(), null, null);

        String hostname = getHostname();
        if (hostname != null && !hostname.isEmpty()) {
            // Ensure we replace any existing vhost option with the redirect version.
            queryOptions = new LinkedHashMap<>(queryOptions);
            queryOptions.put("amqp.vhost", hostname);
        }

        return URISupport.applyParameters(result, queryOptions);
    }

    @Override
    public String toString() {
        try {
            return toURI().toString();
        } catch (Exception ex) {
            return "<Invalid-Redirect-Value>";
        }
    }
}
