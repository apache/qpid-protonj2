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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

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
public class ProtonSession implements Session {

    private static final Logger LOG = LoggerFactory.getLogger(ProtonSession.class);

    private CompletableFuture<Session> openFuture = new CompletableFuture<Session>();
    private CompletableFuture<Session> closeFuture = new CompletableFuture<Session>();

    private final ProtonSessionOptions options;
    private final ProtonConnection connection;
    private final org.apache.qpid.proton4j.engine.Session session;
    private final ScheduledExecutorService executor;

    public ProtonSession(ProtonSessionOptions options, ProtonConnection connection, org.apache.qpid.proton4j.engine.Session session) {
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
        return createReceiver(address, new ProtonReceiverOptions());
    }

    @Override
    public Receiver createReceiver(String address, ReceiverOptions receiverOptions) {
        return new ProtonReceiver(receiverOptions, this, session.receiver(address)).open();
    }

    @Override
    public Sender createSender(String address) {
        return createSender(address, new ProtonSenderOptions());
    }

    @Override
    public Sender createSender(String address, SenderOptions senderOptions) {
        return new ProtonSender(senderOptions, this, session.sender(address)).open();
    }

    //----- Internal API

    ProtonSession open() {
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
