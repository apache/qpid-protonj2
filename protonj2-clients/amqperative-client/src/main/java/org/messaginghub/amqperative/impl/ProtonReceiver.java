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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.AmqpValue;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Decoder;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.messaginghub.amqperative.Delivery;
import org.messaginghub.amqperative.DeliveryState;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.ReceiverOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ProtonReceiver implements Receiver {

    private static final Logger LOG = LoggerFactory.getLogger(ProtonReceiver.class);

    private CompletableFuture<Receiver> openFuture = new CompletableFuture<Receiver>();
    private CompletableFuture<Receiver> closeFuture = new CompletableFuture<Receiver>();

    private final ProtonReceiverOptions options;
    private final ProtonSession session;
    private final org.apache.qpid.proton4j.engine.Receiver receiver;
    private final ScheduledExecutorService executor;

    private final FifoMessageQueue messageQueue;

    public ProtonReceiver(ReceiverOptions options, ProtonSession session, org.apache.qpid.proton4j.engine.Receiver receiver) {
        this.options = new ProtonReceiverOptions(options);
        this.session = session;
        this.receiver = receiver;
        this.executor = session.getScheduler();
        if(options.getCreditWindow() > 0) {
            receiver.setCredit(options.getCreditWindow());
        }
        this.messageQueue = new FifoMessageQueue(options.getCreditWindow());
        messageQueue.start();
    }

    @Override
    public Future<Receiver> openFuture() {
        return openFuture;
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
        //TODO: verify timeout conventions align
        try {
            return messageQueue.dequeue(timeout);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);//TODO: better exception
        }
    }

    @Override
    public Future<Receiver> close() {
        return closeFuture;
    }

    @Override
    public Future<Receiver> detach() {
        return closeFuture;
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

    //----- Internal API

    ProtonReceiver open() {
        executor.execute(() -> {
            receiver.openHandler(result -> {
                if (result.succeeded()) {
                    openFuture.complete(this);
                    LOG.trace("Receiver opened successfully");
                } else {
                    openFuture.completeExceptionally(result.error());
                    LOG.error("Receiver failed to open: ", result.error());
                }
            });
            receiver.deliveryReceivedEventHandler(d -> {
                ProtonBuffer buffer = d.readAll();

                Decoder decoder = CodecFactory.getDefaultDecoder();
                DecoderState decoderState = decoder.newDecoderState();

                Object o;
                try {
                    //TODO: Decode failed with netty ref count violation if decode was deferred until removing
                    //      the IncomingDelivery (rather than imperative Delivery with decoded message as now done)
                    //      from the queue, suggests work is needed on lifecycle of the payload buffer.
                    o = decoder.readObject(buffer, decoderState);
                } catch (IOException e) {
                    throw new IllegalStateException(e);//TODO: better exception
                }

                Object body = ((AmqpValue) o).getValue();

                Message msg  = Message.create(body);

                Delivery del = new Delivery() {

                    @Override
                    public Delivery settle() {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public boolean isRemotelySettled() {
                        // TODO Auto-generated method stub
                        return false;
                    }

                    @Override
                    public byte[] getTag() {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public DeliveryState getRemoteState() {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public int getMessageFormat() {
                        // TODO Auto-generated method stub
                        return 0;
                    }

                    @Override
                    public Message getMessage() {
                        return msg;
                    }

                    @Override
                    public DeliveryState getLocalState() {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public Delivery disposition(DeliveryState state, boolean settle) {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public Delivery accept() {
                        executor.execute(() -> {
                            d.disposition(Accepted.getInstance(), true);

                            //TODO: only do if the credit window is set
                            //TODO: proper replenishment
                            receiver.setCredit( receiver.getCredit() + 1);
                        });
                        return this;
                    }
                };

                messageQueue.enqueue(del);
            });
            receiver.open();
        });

        return this;
    }
}
