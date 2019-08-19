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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.messaginghub.amqperative.Client;
import org.messaginghub.amqperative.Connection;
import org.messaginghub.amqperative.ConnectionOptions;
import org.messaginghub.amqperative.ContainerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container of {@link Connection} instances that are all created with the same
 * container parent and therefore share the same container Id.
 */
public class ClientInstance implements Client {

    private static final Logger LOG = LoggerFactory.getLogger(ClientInstance.class);

    private final ClientContainerOptions options;
    private final Map<String, ClientConnection> connections = new HashMap<>();

    /**
     * @param options
     *      The container options to use to configure this container instance.
     */
    public ClientInstance(ContainerOptions options) {
        this.options = new ClientContainerOptions(options);
    }

    @Override
    public Connection createConnection(String hostname, int port) {
        return new ClientConnection(this, new ClientConnectionOptions(hostname, port)).connect();
    }

    @Override
    public Connection createConnection(String hostname, int port, ConnectionOptions options) {
        return new ClientConnection(this, new ClientConnectionOptions(hostname, port, options)).connect();
    }

    @Override
    public String getContainerId() {
        return options.getContainerId();
    }

    @Override
    public Client stop() {
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
        return this;
    }

    @Override
    public boolean isStopped() {
        return true;
    }

    //----- Internal API

    void addConnection(ClientConnection connection) {
        synchronized (connections) {
            checkStopped();
            this.connections.put(connection.getId(), connection);
        }
    }

    void removeConnection(ClientConnection connection) {
        synchronized (connections) {
            checkStopped();
            this.connections.remove(connection.getId());
        }
    }

    void checkStopped() {
        if (isStopped()) {
            throw new IllegalStateException("Container has already been stopped.");
        }
    }
}
