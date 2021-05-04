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
package org.apache.qpid.protonj2.client.impl;

import static org.apache.qpid.protonj2.client.impl.ClientConstants.ADDRESS;
import static org.apache.qpid.protonj2.client.impl.ClientConstants.NETWORK_HOST;
import static org.apache.qpid.protonj2.client.impl.ClientConstants.OPEN_HOSTNAME;
import static org.apache.qpid.protonj2.client.impl.ClientConstants.PATH;
import static org.apache.qpid.protonj2.client.impl.ClientConstants.PORT;
import static org.apache.qpid.protonj2.client.impl.ClientConstants.SCHEME;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.qpid.protonj2.types.Symbol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the AMQP Redirect Map
 */
public final class ClientRedirect {

    private static final Logger LOG = LoggerFactory.getLogger(ClientRedirect.class);

    private final Map<Symbol, Object> redirect;

    private URI cachedURI;

    ClientRedirect(Map<Symbol, Object> redirect) {
        this.redirect = redirect;
    }

    /**
     * Validate the information conveyed in the redirect and signal an error if it is invalid.
     *
     * @return this {@link ClientRedirect} instance which can be assumed carries valid data.
     *
     * @throws Exception if an error occurs during validation of the redirect payload.
     */
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

        // Check it actually converts to URI since we require it do so later
        cachedURI = toURI();

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
        return (String) redirect.get(SCHEME);
    }

    /**
     * @return the path that the remote indicated should be path of the redirect URI.
     */
    public String getPath() {
        return (String) redirect.get(PATH);
    }

    /**
     * @return the address that the remote indicated should be used for link redirection.
     */
    public String getAddress() {
        return (String) redirect.get(ADDRESS);
    }

    /**
     * Construct a URI from the redirection information available.
     *
     * @return a URI that matches the redirection information provided.
     *
     * @throws Exception if an error occurs construct a URI from the redirection information.
     */
    public URI toURI() throws Exception {
        if (cachedURI != null) {
            return cachedURI;
        } else {
            return cachedURI = new URI(getScheme(), null, getNetworkHost(), getPort(), getPath(), null, null);
        }
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
