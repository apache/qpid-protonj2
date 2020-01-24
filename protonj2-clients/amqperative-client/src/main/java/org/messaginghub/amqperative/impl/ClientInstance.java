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
package org.messaginghub.amqperative.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.messaginghub.amqperative.Client;
import org.messaginghub.amqperative.ClientOptions;
import org.messaginghub.amqperative.Connection;
import org.messaginghub.amqperative.ConnectionOptions;
import org.messaginghub.amqperative.futures.ClientFutureFactory;
import org.messaginghub.amqperative.impl.exceptions.ClientClosedException;
import org.messaginghub.amqperative.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container of {@link Connection} instances that are all created with the same
 * container parent and therefore share the same container Id.
 */
public final class ClientInstance implements Client {

    private static final Logger LOG = LoggerFactory.getLogger(ClientInstance.class);

    private static final IdGenerator CONTAINER_ID_GENERATOR = new IdGenerator();

    private final AtomicInteger CONNECTION_COUNTER = new AtomicInteger();
    private final ClientOptions options;
    private final ConnectionOptions defaultConnectionOptions = new ConnectionOptions();
    private final Map<String, ClientConnection> connections = new HashMap<>();
    private final String clientUniqueId = CONTAINER_ID_GENERATOR.generateId();

    private volatile boolean closed;

    public static ClientInstance create() {
        return new ClientInstance(new ClientOptions());
    }

    public static ClientInstance create(ClientOptions options) {
        Objects.requireNonNull(options, "Client options must be non-null");
        Objects.requireNonNull(options.id(), "User supplied container Id must be non-null");

        return new ClientInstance(new ClientOptions(options));
    }

    /**
     * @param options
     *      The container options to use to configure this container instance.
     */
    ClientInstance(ClientOptions options) {
        this.options = options;
    }

    @Override
    public synchronized Connection connect(String host, int port) throws ClientException {
        checkClosed();
        return addConnection(new ClientConnection(this, host, port, defaultConnectionOptions).connect().open());
    }

    @Override
    public synchronized Connection connect(String host, int port, ConnectionOptions options) throws ClientException {
        checkClosed();
        return addConnection(new ClientConnection(this, host, port, new ConnectionOptions(options)).connect().open());
    }

    @Override
    public synchronized Connection connect(String host) throws ClientException {
        checkClosed();
        return addConnection(new ClientConnection(this, host, -1, defaultConnectionOptions).connect().open());
    }

    @Override
    public synchronized Connection connect(String host, ConnectionOptions options) throws ClientException {
        checkClosed();
        return addConnection(new ClientConnection(this, host, -1, new ConnectionOptions(options)).connect().open());
    }

    @Override
    public String containerId() {
        return options.id();
    }

    String getClientUniqueId() {
        return clientUniqueId;
    }

    ClientOptions options() {
        return options;
    }

    @Override
    public synchronized Future<Client> close() {
        if (!closed) {
            closed = true;

            List<Connection> connectionsView = new ArrayList<>(connections.values());
            connectionsView.forEach((connection) -> connection.close());

            for (Connection connection : connectionsView) {
                try {
                    connection.close().get();
                } catch (Throwable ignored) {
                    LOG.trace("Error while closing connection, ignoring", ignored);
                }
            }

            //TODO: await the actual futures above after starting the process.
            //      we don't have a future that can aggregate the futures from above.
            return ClientFutureFactory.completedFuture(this);
        } else {
            return ClientFutureFactory.completedFuture(this);
        }
    }

    //----- Internal API

    private void checkClosed() throws ClientClosedException {
        if (closed) {
            throw new ClientClosedException("Cannot create new connections, the Client has been closed.");
        }
    }

    String nextConnectionId() {
        return getClientUniqueId() + ":" + CONNECTION_COUNTER.incrementAndGet();
    }

    private ClientConnection addConnection(ClientConnection connection) {
        this.connections.put(connection.getId(), connection);
        return connection;
    }

    void unregisterConnection(ClientConnection connection) {
        synchronized (connections) {
            this.connections.remove(connection.getId());
        }
    }
}
