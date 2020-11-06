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

import java.util.Map;

import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.apache.qpid.protonj2.client.SessionOptions;
import org.apache.qpid.protonj2.client.StreamReceiver;
import org.apache.qpid.protonj2.client.StreamSender;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientUnsupportedOperationException;
import org.apache.qpid.protonj2.engine.Session;

/**
 * A specialized {@link ClientSession} that is the parent of a {@link ClientStreamSender} or
 * {@link ClientStreamReceiver} and cannot create any further resources as the lifetime of the
 * session is tied to the child {@link StreamSender} or {@link StreamReceiver}.
 */
public final class ClientStreamSession extends ClientSession {

    public ClientStreamSession(ClientConnection connection, SessionOptions options, String sessionId, Session session) {
        super(connection, options, sessionId, session);
    }

    @Override
    public Receiver openReceiver(String address) throws ClientException {
        return openReceiver(address, null);
    }

    @Override
    public Receiver openReceiver(String address, ReceiverOptions receiverOptions) throws ClientException {
        checkClosedOrFailed();
        throw new ClientUnsupportedOperationException("Cannot create a receiver from a streaming resource session");
    }

    @Override
    public Receiver openDurableReceiver(String address, String subscriptionName) throws ClientException {
        checkClosedOrFailed();
        throw new ClientUnsupportedOperationException("Cannot create a receiver from a streaming resource session");
    }

    @Override
    public Receiver openDurableReceiver(String address, String subscriptionName, ReceiverOptions receiverOptions) throws ClientException {
        checkClosedOrFailed();
        throw new ClientUnsupportedOperationException("Cannot create a receiver from a streaming resource session");
    }

    @Override
    public Receiver openDynamicReceiver() throws ClientException {
        checkClosedOrFailed();
        throw new ClientUnsupportedOperationException("Cannot create a receiver from a streaming resource session");
    }

    @Override
    public Receiver openDynamicReceiver(Map<String, Object> dynamicNodeProperties) throws ClientException {
        checkClosedOrFailed();
        throw new ClientUnsupportedOperationException("Cannot create a receiver from a streaming resource session");
    }

    @Override
    public Receiver openDynamicReceiver(ReceiverOptions receiverOptions) throws ClientException {
        checkClosedOrFailed();
        throw new ClientUnsupportedOperationException("Cannot create a receiver from a streaming resource session");
    }

    @Override
    public Receiver openDynamicReceiver(Map<String, Object> dynamicNodeProperties, ReceiverOptions receiverOptions) throws ClientException {
        checkClosedOrFailed();
        throw new ClientUnsupportedOperationException("Cannot create a receiver from a streaming resource session");
    }

    @Override
    public Sender openSender(String address) throws ClientException {
        checkClosedOrFailed();
        throw new ClientUnsupportedOperationException("Cannot create a receiver from a streaming resource session");
    }

    @Override
    public Sender openSender(String address, SenderOptions senderOptions) throws ClientException {
        checkClosedOrFailed();
        throw new ClientUnsupportedOperationException("Cannot create a receiver from a streaming resource session");
    }

    @Override
    public Sender openAnonymousSender() throws ClientException {
        checkClosedOrFailed();
        throw new ClientUnsupportedOperationException("Cannot create a receiver from a streaming resource session");
    }

    @Override
    public Sender openAnonymousSender(SenderOptions senderOptions) throws ClientException {
        checkClosedOrFailed();
        throw new ClientUnsupportedOperationException("Cannot create a receiver from a streaming resource session");
    }
}
