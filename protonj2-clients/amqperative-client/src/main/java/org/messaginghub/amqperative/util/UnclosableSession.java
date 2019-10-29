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
package org.messaginghub.amqperative.util;

import java.util.Objects;
import java.util.concurrent.Future;

import org.messaginghub.amqperative.Client;
import org.messaginghub.amqperative.Connection;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.ReceiverOptions;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.Session;
import org.messaginghub.amqperative.impl.ClientException;
import org.messaginghub.amqperative.impl.ClientSession;

/**
 * A Session proxy that will not allow the caller to close the {@link Session} as it is
 * owned by the creating object and its life-cycle managed there was well.
 */
public final class UnclosableSession implements Session {

    private final ClientSession session;

    public UnclosableSession(ClientSession session) {
        Objects.requireNonNull(session, "Cannot wrap a null session instance");
        this.session = session;
    }

    @Override
    public Client getClient() {
        return session.getClient();
    }

    @Override
    public Connection getConnection() {
        return session.getConnection();
    }

    @Override
    public Future<Session> openFuture() {
        return session.openFuture();
    }

    @Override
    public Future<Session> close() {
        throw new UnsupportedOperationException("Cannot close this session instance, only its parent can");
    }

    @Override
    public Receiver openReceiver(String address) throws ClientException {
        return session.openReceiver(address);
    }

    @Override
    public Receiver openReceiver(String address, ReceiverOptions receiverOptions) throws ClientException {
        return session.openReceiver(address, receiverOptions);
    }

    @Override
    public Receiver openDynamicReceiver() throws ClientException {
        return session.openDynamicReceiver();
    }

    @Override
    public Receiver openDynamicReceiver(ReceiverOptions receiverOptions) throws ClientException {
        return session.openDynamicReceiver(receiverOptions);
    }

    @Override
    public Sender openSender(String address) throws ClientException {
        return session.openSender(address);
    }

    @Override
    public Sender openSender(String address, SenderOptions senderOptions) throws ClientException {
        return session.openSender(address, senderOptions);
    }

    @Override
    public Sender openAnonymousSender() throws ClientException {
        return session.openAnonymousSender();
    }

    @Override
    public Sender openAnonymousSender(SenderOptions senderOptions) throws ClientException {
        return session.openAnonymousSender(senderOptions);
    }
}
