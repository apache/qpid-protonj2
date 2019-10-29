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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.messaginghub.amqperative.Client;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.Session;
import org.messaginghub.amqperative.Tracker;
import org.messaginghub.amqperative.impl.ClientException;
import org.messaginghub.amqperative.impl.ClientSender;

/**
 * A Sender proxy that will not allow the caller to close the {@link Sender} as it is
 * owned by the creating object and its life-cycle managed there was well.
 */
public final class UnclosableSender implements Sender {

    private final ClientSender sender;

    public UnclosableSender(ClientSender sender) {
        this.sender = sender;
    }

    @Override
    public Client getClient() {
        return sender.getClient();
    }

    @Override
    public Session getSession() {
        return sender.getSession();
    }

    @Override
    public Tracker send(Message<?> message) throws ClientException {
        return sender.send(message);
    }

    @Override
    public Tracker trySend(Message<?> message) throws ClientException {
        return sender.trySend(message);
    }

    @Override
    public Future<Sender> openFuture() {
        return sender.openFuture();
    }

    @Override
    public Future<Sender> close() {
        throw new UnsupportedOperationException("Cannot close this Sender instance, only the parent reousce can");
    }

    @Override
    public Future<Sender> detach() {
        throw new UnsupportedOperationException("Cannot detach this Sender instance, only the parent reousce can");
    }

    @Override
    public Tracker send(Message<?> message, Consumer<Tracker> onUpdated) {
        return sender.send(message, onUpdated);
    }

    @Override
    public Tracker send(Message<?> message, Consumer<Tracker> onUpdated, ExecutorService executor) {
        return sender.send(message, onUpdated, executor);
    }
}
