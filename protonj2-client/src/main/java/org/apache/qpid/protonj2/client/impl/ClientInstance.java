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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.ClientOptions;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.client.futures.ClientFutureFactory;
import org.apache.qpid.protonj2.client.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container of {@link Connection} instances that are all created with the same
 * container parent and therefore share the same container Id.
 */
public final class ClientInstance implements Client {

    private static final Logger LOG = LoggerFactory.getLogger(ClientInstance.class);

    private static final IdGenerator CONTAINER_ID_GENERATOR = new IdGenerator();
    private static final ClientFutureFactory FUTURES = ClientFutureFactory.create(ClientFutureFactory.CONSERVATIVE);

    private final AtomicInteger CONNECTION_COUNTER = new AtomicInteger();
    private final ClientOptions options;
    private final ConnectionOptions defaultConnectionOptions = new ConnectionOptions();
    private final Map<String, ClientConnection> connections = new HashMap<>();
    private final String clientUniqueId = CONTAINER_ID_GENERATOR.generateId();
    private final ClientFuture<Client> closedFuture = FUTURES.createFuture();

    private volatile boolean closed;

    /**
     * @return a newly create {@link ClientInstance} that uses default configuration.
     */
    public static ClientInstance create() {
        return new ClientInstance(new ClientOptions());
    }

    /**
     * @param options
     * 		The configuration options to apply to the newly create {@link ClientInstance}.
     *
     * @return a newly create {@link ClientInstance} that uses the configuration provided.
     */
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

    @SuppressWarnings("resource")
    @Override
    public synchronized Connection connect(String host, int port) throws ClientException {
        checkClosed();
        return addConnection(new ClientConnection(this, host, port, defaultConnectionOptions).connect());
    }

    @SuppressWarnings("resource")
    @Override
    public synchronized Connection connect(String host, int port, ConnectionOptions options) throws ClientException {
        checkClosed();
        return addConnection(new ClientConnection(this, host, port, new ConnectionOptions(options)).connect());
    }

    @SuppressWarnings("resource")
    @Override
    public synchronized Connection connect(String host) throws ClientException {
        checkClosed();
        return addConnection(new ClientConnection(this, host, -1, defaultConnectionOptions).connect());
    }

    @SuppressWarnings("resource")
    @Override
    public synchronized Connection connect(String host, ConnectionOptions options) throws ClientException {
        checkClosed();
        return addConnection(new ClientConnection(this, host, -1, new ConnectionOptions(options)).connect());
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
    public void close() {
        try {
            closeAsync().get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.interrupted();
        }
    }

    @Override
    public synchronized Future<Client> closeAsync() {
        if (!closed) {
            closed = true;

            if (connections.isEmpty()) {
                closedFuture.complete(this);
            } else {
                List<Connection> connectionsView = new ArrayList<>(connections.values());
                connectionsView.forEach((connection) -> connection.close());

                for (Connection connection : connectionsView) {
                    try {
                        connection.close();
                    } catch (Throwable ignored) {
                        LOG.trace("Error while closing connection, ignoring", ignored);
                    }
                }
            }
        }

        return closedFuture;
    }

    //----- Internal API

    private void checkClosed() throws ClientIllegalStateException {
        if (closed) {
            throw new ClientIllegalStateException("Cannot create new connections, the Client has been closed.");
        }
    }

    String nextConnectionId() {
        return getClientUniqueId() + ":" + CONNECTION_COUNTER.incrementAndGet();
    }

    private ClientConnection addConnection(ClientConnection connection) {
        connections.put(connection.getId(), connection);
        return connection;
    }

    void unregisterConnection(ClientConnection connection) {
        synchronized (connections) {
            connections.remove(connection.getId());
            if (closed && connections.isEmpty()) {
                closedFuture.complete(this);
            }
        }
    }
}
