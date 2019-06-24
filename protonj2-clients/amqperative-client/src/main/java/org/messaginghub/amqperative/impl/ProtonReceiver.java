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

import org.messaginghub.amqperative.Delivery;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.ReceiverOptions;

/**
 *
 */
public class ProtonReceiver implements Receiver {

    private final ProtonReceiverOptions options;
    private final ProtonSession session;
    private final org.apache.qpid.proton4j.engine.Receiver receiver;

    public ProtonReceiver(ReceiverOptions options, ProtonSession session, org.apache.qpid.proton4j.engine.Receiver receiver) {
        this.options = new ProtonReceiverOptions(options);
        this.session = session;
        this.receiver = receiver;
    }

    @Override
    public Delivery receive() throws IllegalStateException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Delivery tryReceive() throws IllegalStateException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Delivery receive(long timeout) throws IllegalStateException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<Receiver> close() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<Receiver> detach() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getQueueSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Receiver onMessage(Consumer<Delivery> handler) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Receiver onMessage(Consumer<Delivery> handler, ExecutorService executor) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Receiver addCredit(int credits) throws IllegalStateException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<Receiver> drainCredit(long timeout) throws IllegalStateException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }
}
