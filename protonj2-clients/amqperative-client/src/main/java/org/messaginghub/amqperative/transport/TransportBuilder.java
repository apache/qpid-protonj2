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
package org.messaginghub.amqperative.transport;

import java.util.concurrent.ThreadFactory;

import org.messaginghub.amqperative.SslOptions;
import org.messaginghub.amqperative.TransportOptions;

/**
 * Builder of Transport instances that will validate the build options and produce a
 * correctly configured transport based on the options set.
 */
public final class TransportBuilder {

    private String host;
    private int port;
    private TransportOptions options;
    private SslOptions sslOptions;
    private ThreadFactory threadFactory;
    private TransportListener listener;

    public TransportBuilder host(String host) {
        this.host = host;
        return this;
    }

    public TransportBuilder port(int port) {
        this.port = port;
        return this;
    }

    public TransportBuilder transportOptions(TransportOptions options) {
        this.options = options;
        return this;
    }

    public TransportBuilder sslOptions(SslOptions sslOptions) {
        this.sslOptions = sslOptions;
        return this;
    }

    public TransportBuilder threadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
        return this;
    }

    public TransportBuilder transportListener(TransportListener listener) {
        this.listener = listener;
        return this;
    }

    public Transport build() {
        if (options == null) {
            throw new IllegalArgumentException("Transport Options cannot be null");
        }

        if (sslOptions == null) {
            throw new IllegalArgumentException("Transport SSL Options cannot be null");
        }

        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Transport host value cannot be null");
        }

        if (port < 0) {
            port = sslOptions.sslEnabled() ? sslOptions.defaultSslPort() : options.defaultTcpPort();
        }

        if (listener == null) {
            throw new IllegalStateException("TransportListener required to create a new Transport.");
        }

        final TcpTransport transport;

        if (options.useWebSockets()) {
            transport = new WebSocketTransport(host, port, options, sslOptions);
        } else {
            transport = new TcpTransport(host, port, options, sslOptions);
        }

        transport.setTransportListener(listener);
        transport.setThreadFactory(threadFactory);

        return transport;
    }
}
