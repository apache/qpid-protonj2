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

import java.util.Objects;

import org.messaginghub.amqperative.impl.ClientInstance;
import org.messaginghub.amqperative.impl.ClientInstanceOptions;

/**
 * The Container that hosts AMQP Connections
 */
public interface Client {

    /**
     * @return a new {@link Client} instance configured with defaults.
     */
    static Client create() {
        return create(new ClientInstanceOptions());
    }

    /**
     * Create a new {@link Client} instance using provided configuration options.
     *
     * @param options
     * 		The configuration options to use when creating the client.
     *
     * @return a new {@link Client} instance configured using the provided options.
     */
    static Client create(ClientOptions options) {
        Objects.requireNonNull(options, "options must be non-null");
        return new ClientInstance(options);
    }

    /**
     * @return the container id assigned to this {@link Client} instance.
     */
    String getContainerId();

    // TODO: Rename just close, make it retain that state until not closed and then maybe make it stop / start cycle.
    //       closeAll implies races on closeAll and another thread doing an open etc.

    /**
     * Closes all currently open {@link Connection} instances created by this client.
     * <p>
     * This method blocks and waits for each connection to close in turn using the configured
     * close timeout of the {@link ConnectionOptions} that the connection was created with.
     *
     * @return this {@link Client} instance.
     */
    Client closeAll();

    /**
     * Connect to the specified host and port, without credentials and with all
     * connection options set to their defaults.
     *
     * @param host
     *            the host to connect to
     * @param port
     *            the port to connect to
     *
     * @return connection, establishment not yet completed
     */
    Connection connect(String host, int port);

    /**
     * Connect to the specified host and port, with given connection options.
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
