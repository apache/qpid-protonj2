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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.engine.IncomingDelivery;
import org.messaginghub.amqperative.Delivery;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.ReceiverOptions;
import org.messaginghub.amqperative.client.exceptions.ClientExceptionSupport;
import org.messaginghub.amqperative.futures.ClientFuture;
import org.messaginghub.amqperative.util.FifoMessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientReceiver implements Receiver {

    private static final Logger LOG = LoggerFactory.getLogger(ClientReceiver.class);

    private final ClientFuture<Receiver> openFuture;
    private final ClientFuture<Receiver> closeFuture;

    private final ClientReceiverOptions options;
    private final ClientSession session;
    private final org.apache.qpid.proton4j.engine.Receiver receiver;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> failureCause = new AtomicReference<>();

    private final FifoMessageQueue messageQueue;

    public ClientReceiver(ReceiverOptions options, ClientSession session, org.apache.qpid.proton4j.engine.Receiver receiver) {
        this.options = new ClientReceiverOptions(options);
        this.session = session;
        this.receiver = receiver;
        this.executor = session.getScheduler();
        this.openFuture = session.getFutureFactory().createFuture();
        this.closeFuture = session.getFutureFactory().createFuture();

        if (options.getCreditWindow() > 0) {
            receiver.setCredit(options.getCreditWindow());
        }

        messageQueue = new FifoMessageQueue(options.getCreditWindow());
        messageQueue.start();
    }

    @Override
    public Future<Receiver> openFuture() {
        return openFuture;
    }

    @Override
    public Delivery receive() throws IllegalStateException {
        //TODO: verify timeout conventions align
        try {
            return messageQueue.dequeue(-1);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);//TODO: better exception
        }
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
    public Delivery tryReceive() throws IllegalStateException {
        try {
            return messageQueue.dequeue(0);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);//TODO: better exception
        }
    }

    @Override
    public Future<Receiver> close() {
        if (closed.compareAndSet(false, true) && !openFuture.isFailed()) {
            executor.execute(() -> {
                receiver.close();
            });
        }
        return closeFuture;
    }

    @Override
    public Future<Receiver> detach() {
        return closeFuture;
    }

    @Override
    public long getQueueSize() {
        return messageQueue.size();
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
        executor.execute(() -> {
            // TODO - This is just a set without addition for now
            receiver.setCredit(credits);
        });

        return this;
    }

    @Override
    public Future<Receiver> drainCredit(long timeout) throws IllegalStateException, IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    //----- Internal API

    void disposition(IncomingDelivery delivery, DeliveryState state, boolean settled) {
        executor.execute(() -> {
            delivery.disposition(state, settled);
        });
    }

    ClientReceiver open() {
        executor.execute(() -> {
            receiver.openHandler(result -> {
                if (result.succeeded()) {
                    openFuture.complete(this);
                    LOG.trace("Receiver opened successfully");
                } else {
                    openFuture.failed(ClientExceptionSupport.createNonFatalOrPassthrough(result.error()));
                    LOG.error("Receiver failed to open: ", result.error());
                }
            });
            receiver.closeHandler(result -> {
                closed.set(true);
                closeFuture.complete(this);
            });

            receiver.deliveryReceivedEventHandler(delivery -> {
                messageQueue.enqueue(new ClientDelivery(this, delivery));
            });

            receiver.deliveryUpdatedEventHandler(delivery -> {
               LOG.info("Delivery was updated: ", delivery);
               // TODO - event or other reaction
            });

            receiver.detachHandler(receiver -> {
                LOG.info("Receiver link remotely detached: ", receiver);
            });

            receiver.open();
        });

        return this;
    }

    void setFailureCause(Throwable failureCause) {
        this.failureCause.set(failureCause);
    }

    Throwable getFailureCause() {
        if (failureCause.get() == null) {
            return session.getFailureCause();
        }

        return failureCause.get();
    }

    //----- Private implementation details

    private void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            IllegalStateException error = null;

            if (getFailureCause() == null) {
                error = new IllegalStateException("The Receiver is closed");
            } else {
                error = new IllegalStateException("The Receiver was closed due to an unrecoverable error.");
                error.initCause(getFailureCause());
            }

            throw error;
        }
    }
}
