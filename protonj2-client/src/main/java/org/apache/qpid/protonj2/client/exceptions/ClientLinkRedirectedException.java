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
package org.apache.qpid.protonj2.client.exceptions;

import java.net.URI;

import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.impl.ClientRedirect;
import org.apache.qpid.protonj2.types.transport.Open;

/**
 * A {@link ClientLinkRemotelyClosedException} type that defines that the remote peer has requested that
 * this link be redirected to some alternative peer.  The redirect information can be obtained
 * by calling the {@link ClientLinkRedirectedException#getRedirectionURI()} method which
 * return the URI of the peer the client is being redirect to.  The address is also accessible
 * for use in creating a new link after reconnection.
 */
public class ClientLinkRedirectedException extends ClientLinkRemotelyClosedException {

    private static final long serialVersionUID = 5872211116061710369L;

    private final ClientRedirect redirect;

    public ClientLinkRedirectedException(String reason, ClientRedirect redirect, ErrorCondition condition) {
        super(reason, condition);

        this.redirect = redirect;
    }

    /**
     * the host name of the remote peer where the {@link Sender} or {@link Receiver} is being
     * redirected.
     * <p>
     * This value should be used in the 'hostname' field of the {@link Open} frame, and
     * during SASL negotiation (if used).  When using this client to reconnect this value
     * would be assigned to the {@link ConnectionOptions#virtualHost(String)} value in the
     * {@link ConnectionOptions} passed to the newly created {@link Connection}.
     *
     * @return the host name of the remote AMQP container to redirect to.
     */
    public String getHostname() {
        return redirect.getHostname();
    }

    /**
     * A network level host name that matches either the DNS hostname or IP address of the
     * remote container.
     *
     * @return the network level host name value where the connection is being redirected.
     */
    public String getNetworkHost() {
        return redirect.getNetworkHost();
    }

    /**
     * A network level port value that should be used when redirecting this connection.
     *
     * @return the network port value where the connection is being redirected.
     */
    public int getPort() {
        return redirect.getPort();
    }

    /**
     * Returns the connection scheme that should be used when connecting to the remote
     * host and port provided in this redirection.
     *
     * @return the connection scheme to use when redirecting to the provided host and port.
     */
    public String getScheme() {
        return redirect.getScheme();
    }

    /**
     * The path value that should be used when connecting to the provided host and port.
     *
     * @return the path value that should be used when redirecting to the provided host and port.
     */
    public String getPath() {
        return redirect.getPath();
    }

    /**
     * The address value that should be used when connecting to the provided host and port and
     * creating a new link instance as directed.
     *
     * @return the address value that should be used when redirecting to the provided host and port.
     */
    public String getAddress() {
        return redirect.getAddress();
    }

    /**
     * Attempt to construct a URI that represents the location where the redirect is
     * sending the client {@link Sender} or {@link Receiver}.
     *
     * @return the URI that represents the redirection.
     *
     * @throws Exception if an error occurs while converting the redirect into a URI.
     */
    public URI getRedirectionURI() throws Exception {
        return redirect.toURI();
    }
}
