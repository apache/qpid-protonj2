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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.engine.IncomingDelivery;
import org.apache.qpid.proton4j.engine.LinkState;
import org.messaginghub.amqperative.Client;
import org.messaginghub.amqperative.Delivery;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.ReceiverOptions;
import org.messaginghub.amqperative.Session;
import org.messaginghub.amqperative.futures.ClientFuture;
import org.messaginghub.amqperative.util.FifoMessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientReceiver implements Receiver {

    private static final Logger LOG = LoggerFactory.getLogger(ClientReceiver.class);

    private static final AtomicIntegerFieldUpdater<ClientReceiver> CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientReceiver.class, "closed");

    private final ClientFuture<Receiver> openFuture;
    private final ClientFuture<Receiver> closeFuture;

    private final ClientReceiverOptions options;
    private final ClientSession session;
    private final org.apache.qpid.proton4j.engine.Receiver protonReceiver;
    private final ScheduledExecutorService executor;
    private final AtomicReference<Throwable> failureCause = new AtomicReference<>();
    private final String receiverId;
    private final FifoMessageQueue messageQueue;
    private volatile int closed;

    public ClientReceiver(ReceiverOptions options, ClientSession session, String address) {
        this.options = new ClientReceiverOptions(options);
        this.session = session;
        this.receiverId = session.nextReceiverId();
        this.executor = session.getScheduler();
        this.openFuture = session.getFutureFactory().createFuture();
        this.closeFuture = session.getFutureFactory().createFuture();

        if (options.getLinkName() != null) {
            this.protonReceiver = session.getProtonSession().receiver(options.getLinkName());
        } else {
            this.protonReceiver = session.getProtonSession().receiver("receiver-" + getId());
        }

        this.options.configureReceiver(protonReceiver, address);

        if (options.getCreditWindow() > 0) {
            protonReceiver.setCredit(options.getCreditWindow());
        }

        messageQueue = new FifoMessageQueue(options.getCreditWindow());
        messageQueue.start();
    }

    @Override
    public Client getClient() {
        return session.getClient();
    }

    @Override
    public Session getSession() {
        return session;
    }

    @Override
    public Future<Receiver> openFuture() {
        return openFuture;
    }

    @Override
    public Delivery receive() throws IllegalStateException {
        checkClosed();
        //TODO: verify timeout conventions align
        try {
            return messageQueue.dequeue(-1);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);//TODO: better exception
        }
    }

    @Override
    public Delivery receive(long timeout) throws IllegalStateException {
        checkClosed();
        //TODO: verify timeout conventions align
        try {
            return messageQueue.dequeue(timeout);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);//TODO: better exception
        }
    }

    @Override
    public Delivery tryReceive() throws IllegalStateException {
        checkClosed();
        try {
            return messageQueue.dequeue(0);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);//TODO: better exception
        }
    }

    @Override
    public Future<Receiver> close() {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1) && !openFuture.isFailed()) {
            executor.execute(() -> {
                protonReceiver.close();
            });
        }
        return closeFuture;
    }

    @Override
    public Future<Receiver> detach() {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1) && !openFuture.isFailed()) {
            executor.execute(() -> {
                protonReceiver.detach();
            });
        }
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
        checkClosed();

        executor.execute(() -> {
            // TODO - This is just a set without addition for now
            protonReceiver.setCredit(credits);
        });

        return this;
    }

    @Override
    public Future<Receiver> drain() {
        checkClosed();
        ClientFuture<Receiver> drained = session.getFutureFactory().createFuture();

        executor.execute(() -> {
            // TODO
            protonReceiver.drain();
        });

        return drained;
    }

    //----- Internal API

    void disposition(IncomingDelivery delivery, DeliveryState state, boolean settled) {
        checkClosed();
        executor.execute(() -> {
            delivery.disposition(state, settled);
        });
    }

    ClientReceiver open() {
        executor.execute(() -> {
            protonReceiver.openHandler(receiver -> handleRemoteOpen(receiver))
                          .closeHandler(receiver -> handleRemoteCloseOrDetach(receiver))
                          .detachHandler(receiver -> handleRemoteCloseOrDetach(receiver))
                          .deliveryUpdatedEventHandler(delivery -> handleDeliveryRemotelyUpdated(delivery))
                          .deliveryReceivedEventHandler(delivery -> handleDeliveryReceivied(delivery))
                          .receiverDrainedEventHandler(receiver -> handleReceiverReportsDrained(receiver))
                          .open();
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

    String getId() {
        return receiverId;
    }

    boolean isClosed() {
        return closed > 0;
    }

    org.apache.qpid.proton4j.engine.Receiver getProtonReceiver() {
        return protonReceiver;
    }

    //----- Handlers for proton receiver events

    private void handleRemoteOpen(org.apache.qpid.proton4j.engine.Receiver receiver) {
        // Check for deferred close pending and hold completion if so
        if (receiver.getRemoteSource() != null) {
            openFuture.complete(this);
            LOG.trace("Receiver opened successfully");
        } else {
            LOG.debug("Receiver opened but remote signalled close is pending: ", receiver);
        }
    }

    private void handleRemoteCloseOrDetach(org.apache.qpid.proton4j.engine.Receiver receiver) {
        CLOSED_UPDATER.lazySet(this, 1);

        // Close should be idempotent so we can just respond here with a close in case
        // of remotely closed sender.  We should set error state from remote though
        // so client can see it.
        try {
            if (receiver.getRemoteState() == LinkState.CLOSED) {
                LOG.info("Sender link remotely closed: ", receiver);
                receiver.close();
            } else {
                LOG.info("Sender link remotely detached: ", receiver);
                receiver.detach();
            }
        } catch (Throwable ignored) {
            LOG.trace("Error while processing remote close event: ", ignored);
        }

        // TODO - Error open future if remote indicated open would fail using an appropriate
        //        exception based on remote error condition if one is set.
        if (receiver.getRemoteSource() == null) {
            openFuture.failed(new ClientException("Link creation was refused"));
        } else {
            openFuture.complete(this);
        }
        closeFuture.complete(this);
    }

    private void handleDeliveryReceivied(IncomingDelivery delivery) {
        LOG.debug("Delivery was updated: ", delivery);
        messageQueue.enqueue(new ClientDelivery(this, delivery));
    }

    private void handleDeliveryRemotelyUpdated(IncomingDelivery delivery) {
        LOG.debug("Delivery was updated: ", delivery);
        // TODO - event or other reaction
    }

    private void handleReceiverReportsDrained(org.apache.qpid.proton4j.engine.Receiver receiver) {
        LOG.debug("Receiver reports drained: ", receiver);
        // TODO - event or other reaction
    }

    //----- Private implementation details

    private void checkClosed() throws IllegalStateException {
        if (isClosed()) {
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
