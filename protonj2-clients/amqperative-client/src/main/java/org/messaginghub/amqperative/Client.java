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

import java.net.URI;
import java.util.Objects;

import org.messaginghub.amqperative.client.ClientInstanceOptions;
import org.messaginghub.amqperative.client.ClientInstance;

/**
 * The Container that hosts AMQP Connections
 */
public interface Client {

    static Client create() {
        return create(new ClientInstanceOptions());
    }

    static Client create(ClientOptions options) {
        Objects.requireNonNull(options, "options must be non-null");
        return new ClientInstance(options);
    }

    /**
     * @return the container id assigned to this {@link Client} instance.
     */
    String getContainerId();

    /**
     * Closes all currently open {@link Connection} instances created by this client.
     *
     * @return this {@link Client} instance.
     */
    Client closeAll();

    /**
     * Connect to the specified remote using the given {@link URI} and configures
     * the connection options using the parameters encoded in the query portion of
     * the given {@link URI}.
     *
     * @param remoteUri
     *            the {@link URI} of the remote to connect to.
     *
     * @return connection, establishment not yet completed
     */
    Connection connect(URI remoteUri);

    /**
     * Connect to the specified remote using the given {@link URI} and configures
     * the connection options using the parameters encoded in the query portion of
     * the given {@link URI}.
     *
     * @param remoteUri
     *            the {@link URI} of the remote to connect to.
     * @param options
     *            options to use when creating the connection.
     *
     * @return connection, establishment not yet completed
     */
    Connection connect(URI remoteUri, ConnectionOptions options);

    /**
     * Connect to the specified host and port, without credentials.
     *
     * @param host
     *            the host to connect to
     * @param port
     *            the port to connect to
     * @return connection, establishment not yet completed
     */
    Connection connect(String host, int port);

    /**
     * Connect to the specified host and port, with given options.
     *
     * @param host
     *            the host to connect to
     * @param port
     *            the port to connect to
     * @param options
     *            options to use when creating the connection.
     *
     * @return connection, establishment not yet completed
     */
    Connection connect(String host, int port, ConnectionOptions options);

}
