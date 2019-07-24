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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.ReceiverOptions;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ClientSession implements Session {

    private static final Logger LOG = LoggerFactory.getLogger(ClientSession.class);

    private CompletableFuture<Session> openFuture = new CompletableFuture<Session>();
    private CompletableFuture<Session> closeFuture = new CompletableFuture<Session>();

    private final ClientSessionOptions options;
    private final ClientConnection connection;
    private final org.apache.qpid.proton4j.engine.Session session;
    private final ScheduledExecutorService executor;

    public ClientSession(ClientSessionOptions options, ClientConnection connection, org.apache.qpid.proton4j.engine.Session session) {
        this.options = options;
        this.connection = connection;
        this.session = session;
        this.executor = connection.getScheduler();
    }

    @Override
    public Future<Session> openFuture() {
        return openFuture;
    }

    @Override
    public Future<Session> close() {
        return closeFuture;
    }

    @Override
    public Receiver createReceiver(String address) {
        return createReceiver(address, new ClientReceiverOptions());
    }

    @Override
    public Receiver createReceiver(String address, ReceiverOptions receiverOptions) {
        String name = receiverOptions.getLinkName();
        if(name == null) {
            //TODO: use container-id + counter rather than UUID?
            name = "reciever-" + UUID.randomUUID();
        }

        //TODO: not thread safe
        org.apache.qpid.proton4j.engine.Receiver receiver = session.receiver(name);

        //TODO: flesh out source
        Source source = new Source();
        source.setAddress(address);

        receiver.setSource(source);
        receiver.setTarget(new Target());

        return new ClientReceiver(receiverOptions, this, receiver).open();
    }

    @Override
    public Sender createSender(String address) {
        return createSender(address, new ClientSenderOptions());
    }

    @Override
    public Sender createSender(String address, SenderOptions senderOptions) {
        String name = senderOptions.getLinkName();
        if(name == null) {
            //TODO: use container-id + counter rather than UUID?
            name = "sender-" + UUID.randomUUID();
        }

        //TODO: this is not thread safe
        org.apache.qpid.proton4j.engine.Sender sender = session.sender(name);

        //TODO: flesh out target
        Target target = new Target();
        target.setAddress(address);

        sender.setTarget(target);
        sender.setSource(new Source());

        return new ClientSender(senderOptions, this, sender).open();
    }

    //----- Internal API

    ClientSession open() {
        executor.execute(() -> {
            session.openHandler(result -> {
                if (result.succeeded()) {
                    openFuture.complete(this);
                    LOG.trace("Connection session opened successfully");
                } else {
                    openFuture.completeExceptionally(result.error());
                    LOG.error("Connection session failed to open: ", result.error());
                }
            });
            session.open();
        });

        return this;
    }

    ScheduledExecutorService getScheduler() {
        return executor;
    }
}
