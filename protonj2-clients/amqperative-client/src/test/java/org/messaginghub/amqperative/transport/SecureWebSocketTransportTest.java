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

import org.messaginghub.amqperative.SslOptions;
import org.messaginghub.amqperative.TransportOptions;
import org.messaginghub.amqperative.transport.TransportListener;
import org.messaginghub.amqperative.transport.WebSocketTransport;

/**
 * Test the WebSocketTransport with channel level security enabled.
 */
public class SecureWebSocketTransportTest extends SslTransportTest {

    @Override
    protected WebSocketTransport createTransport(String host, int port, TransportListener listener, TransportOptions options, SslOptions sslOptions) {
        WebSocketTransport transport = new WebSocketTransport(host, port, options, sslOptions);
        transport.setTransportListener(listener);
        return transport;
    }

    @Override
    protected TransportOptions createTransportOptions() {
        return new TransportOptions().setUseWebSockets(true);
    }

    @Override
    protected TransportOptions createServerTransportOptions() {
        return new TransportOptions().setUseWebSockets(true);
    }
}
