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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.Tracker;

/**
 * Proton based AMQP Sender
 */
public class ProtonSender implements Sender {

    private final ProtonSenderOptions options;
    private final ProtonSession session;
    private final org.apache.qpid.proton4j.engine.Sender sender;

    public ProtonSender(SenderOptions options, ProtonSession session, org.apache.qpid.proton4j.engine.Sender sender) {
        this.options = new ProtonSenderOptions(options);
        this.session = session;
        this.sender = sender;
    }

    @Override
    public Tracker send(Message message) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<Sender> close() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<Sender> detach() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Tracker trySend(Message message, Consumer<Tracker> onUpdated) throws IllegalStateException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Tracker send(Message message, Consumer<Tracker> onUpdated) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Tracker send(Message message, Consumer<Tracker> onUpdated, ExecutorService executor) {
        // TODO Auto-generated method stub
        return null;
    }
}
