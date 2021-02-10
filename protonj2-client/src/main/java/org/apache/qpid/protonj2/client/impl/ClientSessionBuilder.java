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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.SessionOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.Session;

final class ClientSessionBuilder {

    private final AtomicInteger sessionCounter = new AtomicInteger();
    private final ClientConnection connection;
    private final ConnectionOptions connectionOptions;

    private SessionOptions defaultSessionOptions;

    public ClientSessionBuilder(ClientConnection connection) {
        this.connection = connection;
        this.connectionOptions = connection.getOptions();
    }

    public ClientSession session(SessionOptions sessionOptions) throws ClientException {
        final SessionOptions options = sessionOptions != null ? sessionOptions : getDefaultSessionOptions();
        final String sessionId = nextSessionId();
        final Session protonSession = createSession(connection.getProtonConnection(), options);

        return new ClientSession(connection, options, sessionId, protonSession);
    }

    public ClientStreamSession streamSession(SessionOptions sessionOptions) throws ClientException {
        final SessionOptions options = sessionOptions != null ? sessionOptions : getDefaultSessionOptions();
        final String sessionId = nextSessionId();
        final Session protonSession = createSession(connection.getProtonConnection(), options);

        return new ClientStreamSession(connection, options, sessionId, protonSession);
    }

    private static Session createSession(Connection connection, SessionOptions options) {
        return connection.session().setIncomingCapacity(options.incomingCapacity());
    }

    public static Session recreateSession(ClientConnection connection, Session previousSession, SessionOptions options) {
        return connection.getProtonConnection().session().setIncomingCapacity(options.incomingCapacity());
    }

    /*
     * Session options used when none specified by the caller creating a new session.
     */
    public SessionOptions getDefaultSessionOptions() {
        SessionOptions sessionOptions = defaultSessionOptions;
        if (sessionOptions == null) {
            synchronized (this) {
                sessionOptions = defaultSessionOptions;
                if (sessionOptions == null) {
                    sessionOptions = new SessionOptions();
                    sessionOptions.openTimeout(connectionOptions.openTimeout());
                    sessionOptions.closeTimeout(connectionOptions.closeTimeout());
                    sessionOptions.requestTimeout(connectionOptions.requestTimeout());
                    sessionOptions.sendTimeout(connectionOptions.sendTimeout());
                }

                defaultSessionOptions = sessionOptions;
            }
        }

        return sessionOptions;
    }

    String nextSessionId() {
        return connection.getId() + ":" + sessionCounter.incrementAndGet();
    }
}
