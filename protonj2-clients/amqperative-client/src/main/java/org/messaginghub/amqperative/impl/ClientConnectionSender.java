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

import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.futures.ClientFuture;

/**
 * Sender instance used by the connection for sends from the connection itself.
 */
public final class ClientConnectionSender extends ClientSender {

    public ClientConnectionSender(ClientSession session, SenderOptions options, String senderId, org.apache.qpid.proton4j.engine.Sender protonSender) {
        super(session, options, senderId, protonSender);
    }

    @Override
    public ClientFuture<Sender> close() {
        throw new UnsupportedOperationException("Sender owned by the Client Connection cannot be explicity closed.");
    }

    @Override
    ClientConnectionSender open() {
        super.open();
        return this;
    }

    Future<Sender> internalClose() {
        return super.close();
    }
}
