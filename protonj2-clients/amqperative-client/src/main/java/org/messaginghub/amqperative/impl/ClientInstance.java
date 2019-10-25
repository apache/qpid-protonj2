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

    public static ClientInstance create() {
        return new ClientInstance(new ClientOptions());
    }

    public static ClientInstance create(ClientOptions options) {
        Objects.requireNonNull(options, "Client options must be non-null");
        Objects.requireNonNull(options.containerId(), "User supplied container Id must be non-null");

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
    public Connection connect(String host, int port) throws ClientException {
        return new ClientConnection(this, host, port, defaultConnectionOptions).connect().open();
    }

    @Override
    public Connection connect(String host, int port, ConnectionOptions options) throws ClientException {
        return new ClientConnection(this, host, port, new ConnectionOptions(options)).connect().open();
    }

    @Override
    public Connection connect(String host) throws ClientException {
        return new ClientConnection(this, host, -1, defaultConnectionOptions).connect().open();
    }

    @Override
    public Connection connect(String host, ConnectionOptions options) throws ClientException {
        return new ClientConnection(this, host, -1, new ConnectionOptions(options)).connect().open();
    }

    @Override
    public String getContainerId() {
        return options.containerId();
    }

    String getClientUniqueId() {
        return clientUniqueId;
    }

    @Override
    public Future<Client> close() {
        //TODO: prevent new connections being created after calling close

        synchronized (connections) {
            List<Connection> connectionsView = new ArrayList<>(connections.values());
            for (Connection connection : connectionsView) {
                try {
                    connection.close().get();
                } catch (Throwable ignored) {
                    LOG.trace("Error while closing connection, ignoring", ignored);
                }
            }
        }

        //TODO: await the actual futures above after starting the process.
        return ClientFutureFactory.completedFuture(this);
    }

    //----- Internal API

    String nextConnectionId() {
        return getClientUniqueId() + ":" + CONNECTION_COUNTER.incrementAndGet();
    }

    void addConnection(ClientConnection connection) {
        synchronized (connections) {
            this.connections.put(connection.getId(), connection);
        }
    }

    void removeConnection(ClientConnection connection) {
        synchronized (connections) {
            this.connections.remove(connection.getId());
        }
    }
}
