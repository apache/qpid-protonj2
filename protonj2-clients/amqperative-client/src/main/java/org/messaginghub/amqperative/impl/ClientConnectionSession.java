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

import java.util.concurrent.Future;

import org.messaginghub.amqperative.Session;
import org.messaginghub.amqperative.SessionOptions;

/**
 * Connection specific Session instance that offers Connection internal APIs
 */
public final class ClientConnectionSession extends ClientSession {

    ClientConnectionSession(SessionOptions options, ClientConnection connection, org.apache.qpid.proton4j.engine.Session session) {
        super(options, connection, session);
    }

    @Override
    public Future<Session> close() {
        throw new UnsupportedOperationException("Session owned by the Client Connection cannot be explicity closed.");
    }

    Future<Session> internalClose() {
        return super.close();
    }
}
