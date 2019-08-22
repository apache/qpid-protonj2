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

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.qpid.proton4j.engine.Connection;
import org.messaginghub.amqperative.ConnectionOptions;

/**
 * Connection Options for the ProtonConnection implementation
 */
public final class ClientConnectionOptions extends ConnectionOptions {

    private final String hostname;
    private final int port;

    ClientConnectionOptions(String hostname, int port) {
        this(hostname, port, null);
    }

    ClientConnectionOptions(String hostname, int port, ConnectionOptions options) {
        super(options);

        this.hostname = hostname;
        this.port = port;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    /**
     * @return a URI indicating the remote peer to connect to.
     */
    public URI getRemoteURI() {
        try {
            return new URI(null, null, getHostname(), getPort(), null, null, null);
        } catch (URISyntaxException uriEx) {
            throw new IllegalArgumentException("Could not create URI from provided host and port");
        }
    }

    //----- Internal support methods used by the client

    Connection configureConnection(Connection protonConnection) {
        protonConnection.setChannelMax(getChannelMax());
        protonConnection.setMaxFrameSize(getMaxFrameSize());
        protonConnection.setHostname(getHostname());
        protonConnection.setIdleTimeout((int) getIdleTimeout());
        protonConnection.setOfferedCapabilities(ClientConversionSupport.toSymbolArray(getOfferedCapabilities()));
        protonConnection.setDesiredCapabilities(ClientConversionSupport.toSymbolArray(getDesiredCapabilities()));
        protonConnection.setProperties(ClientConversionSupport.toSymbolKeyedMap(getProperties()));

        return protonConnection;
    }
}
