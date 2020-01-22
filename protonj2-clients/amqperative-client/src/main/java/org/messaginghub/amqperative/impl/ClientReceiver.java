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

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
import org.messaginghub.amqperative.Source;
import org.messaginghub.amqperative.Target;
import org.messaginghub.amqperative.futures.ClientFuture;
import org.messaginghub.amqperative.impl.exceptions.ClientExceptionSupport;
import org.messaginghub.amqperative.impl.exceptions.ClientOperationTimedOutException;
import org.messaginghub.amqperative.impl.exceptions.ClientResourceAllocationException;
import org.messaginghub.amqperative.impl.exceptions.ClientResourceClosedException;
import org.messaginghub.amqperative.util.FifoDeliveryQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientReceiver implements Receiver {

    private static final Logger LOG = LoggerFactory.getLogger(ClientReceiver.class);

    private static final AtomicIntegerFieldUpdater<ClientReceiver> CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientReceiver.class, "closed");

    private final ClientFuture<Receiver> openFuture;
    private final ClientFuture<Receiver> closeFuture;

    private final ReceiverOptions options;
    private final ClientSession session;
    private final org.apache.qpid.proton4j.engine.Receiver protonReceiver;
    private final ScheduledExecutorService executor;
    private final AtomicReference<ClientException> failureCause = new AtomicReference<>();
    private final String receiverId;
    private final FifoDeliveryQueue messageQueue;
    private volatile int closed;
    private Consumer<ClientReceiver> receiverRemotelyClosedHandler;

    private volatile Source remoteSource;
    private volatile Target remoteTarget;

    public ClientReceiver(ClientSession session, ReceiverOptions options, String receiverId, org.apache.qpid.proton4j.engine.Receiver receiver) {
        this.options = options;
        this.session = session;
        this.receiverId = receiverId;
        this.executor = session.getScheduler();
        this.openFuture = session.getFutureFactory().createFuture();
        this.closeFuture = session.getFutureFactory().createFuture();
        this.protonReceiver = receiver;

        if (options.creditWindow() > 0) {
            protonReceiver.addCredit(options.creditWindow());
        }

        messageQueue = new FifoDeliveryQueue(options.creditWindow());
        messageQueue.start();
    }

    @Override
    public String address() throws ClientException {
        if (isDynamic()) {
            waitForOpenToComplete();
            return protonReceiver.getRemoteSource().getAddress();
        } else {
            return protonReceiver.getSource() != null ? protonReceiver.getSource().getAddress() : null;
        }
    }

    @Override
    public Source source() throws ClientException {
        waitForOpenToComplete();
        return remoteSource;
    }

    @Override
    public Target target() throws ClientException {
        waitForOpenToComplete();
        return remoteTarget;
    }

    @Override
    public Client client() {
        return session.client();
    }

    @Override
    public Session session() {
        return session;
    }

    ClientReceiver remotelyClosedHandler(Consumer<ClientReceiver> handler) {
        this.receiverRemotelyClosedHandler = handler;
        return this;
    }

    @Override
    public Future<Receiver> openFuture() {
        return openFuture;
    }

    @Override
    public Delivery receive() {
        return receive(-1);
    }

    @Override
    public Delivery receive(long timeout) throws IllegalStateException {
        checkClosed();
        //TODO: verify timeout conventions align
        try {
            Delivery delivery = messageQueue.dequeue(timeout);
            replenishCreditIfNeeded();

            return delivery;
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new IllegalStateException(e);//TODO: better exception
        }
    }

    @Override
    public Delivery tryReceive() throws IllegalStateException {
        checkClosed();

        Delivery delivery = messageQueue.dequeueNoWait();
        replenishCreditIfNeeded();

        return delivery;
    }

    private void replenishCreditIfNeeded() {
        int creditWindow = options.creditWindow();
        if(creditWindow > 0) {
            executor.execute(() -> {
                int currentCredit = protonReceiver.getCredit();
                if (currentCredit <= creditWindow * 0.5) {
                    int potentialPrefetch = currentCredit + messageQueue.size();

                    if (potentialPrefetch <= creditWindow * 0.7) {
                        int additionalCredit = creditWindow - potentialPrefetch;

                        LOG.trace("Consumer granting additional credit: {}", additionalCredit);
                        protonReceiver.addCredit(additionalCredit);
                    }
                }
            });
        }
    }

    @Override
    public Future<Receiver> close() {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            executor.execute(() -> {
                messageQueue.stop();  // Ensure blocked receivers are all unblocked.

                try {
                    protonReceiver.close();
                } catch (Throwable error) {
                    closeFuture.complete(this);
                }

                if (!closeFuture.isDone()) {
                    final long timeout = options.closeTimeout() >= 0 ?
                            options.closeTimeout() : options.requestTimeout();

                    if (timeout > 0) {
                        session.scheduleRequestTimeout(closeFuture, timeout,
                            () -> new ClientOperationTimedOutException("Timed out waiting for Receiver to close"));
                    }
                }
            });
        }
        return closeFuture;
    }

    @Override
    public Future<Receiver> detach() {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1) && !openFuture.isFailed()) {
            executor.execute(() -> {
                try {
                    protonReceiver.detach();
                } catch (Throwable error) {
                    closeFuture.complete(this);
                }

                if (!closeFuture.isDone()) {
                    final long timeout = options.closeTimeout() >= 0 ?
                            options.closeTimeout() : options.requestTimeout();

                    session.scheduleRequestTimeout(closeFuture, timeout,
                        () -> new ClientOperationTimedOutException("Timed out waiting for Receiver to detach"));
                }
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
            // TODO - Are we draining?  If so you've done something wrong...bang!
            protonReceiver.addCredit(credits);
        });

        return this;
    }

    @Override
    public Future<Receiver> drain() {
        checkClosed();
        ClientFuture<Receiver> drained = session.getFutureFactory().createFuture();

        executor.execute(() -> {
            // TODO: If already draining throw IllegalStateException type error as we don't allow stacked drains.
             if (protonReceiver.isDrain()) {
                 drained.failed(new ClientException("Already draining"));
                 return;
             }

             if (protonReceiver.getCredit() == 0) {
                 drained.complete(this);
                 return;
             }

            // TODO: Maybe proton should be returning something here to indicate drain started.
            protonReceiver.drain();
            protonReceiver.drainStateUpdatedHandler(x -> {
                protonReceiver.drainStateUpdatedHandler(null);
                drained.complete(this);
            });
        });

        return drained;
    }

    @Override
    public Map<String, Object> properties() throws ClientException {
        waitForOpenToComplete();
        return ClientConversionSupport.toStringKeyedMap(protonReceiver.getRemoteProperties());
    }

    @Override
    public String[] offeredCapabilities() throws ClientException {
        waitForOpenToComplete();
        return ClientConversionSupport.toStringArray(protonReceiver.getRemoteOfferedCapabilities());
    }

    @Override
    public String[] desiredCapabilities() throws ClientException {
        waitForOpenToComplete();
        return ClientConversionSupport.toStringArray(protonReceiver.getRemoteDesiredCapabilities());
    }

    //----- Internal API

    void disposition(IncomingDelivery delivery, DeliveryState state, boolean settled) {
        checkClosed();
        executor.execute(() -> {
            delivery.disposition(state, settled);
        });
    }

    ClientReceiver open() {
        protonReceiver.openHandler(receiver -> handleRemoteOpen(receiver))
                      .closeHandler(receiver -> handleRemoteCloseOrDetach(receiver))
                      .detachHandler(receiver -> handleRemoteCloseOrDetach(receiver))
                      .deliveryUpdatedHandler(delivery -> handleDeliveryRemotelyUpdated(delivery))
                      .deliveryReceivedHandler(delivery -> handleDeliveryReceived(delivery))
                      .drainStateUpdatedHandler(receiver -> handleReceiverReportsDrained(receiver))
                      .open();

        if (options.openTimeout() > 0) {
            executor.schedule(() -> {
                if (!openFuture.isDone()) {
                    try {
                        protonReceiver.close();
                    } catch (Throwable error) {
                        // Connection will handle all engine errors
                    } finally {
                        failureCause.compareAndSet(null, new ClientOperationTimedOutException(
                            "Receiver attach timed out waiting for remote to open"));
                        CLOSED_UPDATER.lazySet(this, 1);
                        closeFuture.complete(this);
                        openFuture.failed(failureCause.get());
                    }
                }
            }, options.openTimeout(), TimeUnit.MILLISECONDS);
        }

        return this;
    }

    void setFailureCause(ClientException failureCause) {
        this.failureCause.set(failureCause);
    }

    ClientException getFailureCause() {
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

    boolean isDynamic() {
        return protonReceiver.getSource() != null && protonReceiver.getSource().isDynamic();
    }

    org.apache.qpid.proton4j.engine.Receiver getProtonReceiver() {
        return protonReceiver;
    }

    //----- Handlers for proton receiver events

    private void handleRemoteOpen(org.apache.qpid.proton4j.engine.Receiver receiver) {
        // Check for deferred close pending and hold completion if so
        if (receiver.getRemoteSource() != null) {
            if (receiver.getRemoteSource() != null) {
                remoteSource = new RemoteSource(receiver.getRemoteSource());
            }
            if (receiver.getRemoteTarget() != null) {
                remoteTarget = new RemoteTarget(receiver.getRemoteTarget());
            }

            openFuture.complete(this);
            LOG.trace("Receiver opened successfully");
        } else {
            LOG.debug("Receiver opened but remote signalled close is pending: ", receiver);
        }
    }

    private void handleRemoteCloseOrDetach(org.apache.qpid.proton4j.engine.Receiver receiver) {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            // Close should be idempotent so we can just respond here with a close in case
            // of remotely closed sender.  We should set error state from remote though
            // so client can see it.
            try {
                if (protonReceiver.getRemoteState() == LinkState.CLOSED) {
                    LOG.info("Sender link remotely closed: ", receiver);
                    protonReceiver.close();
                } else {
                    LOG.info("Sender link remotely detached: ", receiver);
                    protonReceiver.detach();
                }
            } catch (Throwable ignored) {
                LOG.trace("Error while processing remote close event: ", ignored);
            }

            if (protonReceiver.getRemoteCondition() != null) {
                failureCause.set(ClientErrorSupport.convertToNonFatalException(protonReceiver.getRemoteCondition()));
            } else if (protonReceiver.getRemoteTarget() == null) {
                failureCause.set(new ClientResourceAllocationException("Link creation was refused"));
            } else {
                failureCause.set(new ClientResourceClosedException("The sender has been remotely closed"));
            }

            openFuture.failed(failureCause.get());
            closeFuture.complete(this);

            if (receiverRemotelyClosedHandler != null) {
                receiverRemotelyClosedHandler.accept(this);
            }
        } else {
            closeFuture.complete(this);
        }
    }

    private void handleDeliveryReceived(IncomingDelivery delivery) {
        LOG.debug("Delivery was updated: ", delivery);
        messageQueue.enqueue(new ClientDelivery(this, delivery));
    }

    private void handleDeliveryRemotelyUpdated(IncomingDelivery delivery) {
        LOG.debug("Delivery was updated: ", delivery);
        // TODO - event or other reaction
    }

    private void handleReceiverReportsDrained(org.apache.qpid.proton4j.engine.Receiver receiver) {
        LOG.debug("Receiver reports drained: ", receiver);
        // TODO - event or other reaction, complete saved 'drained' future
    }

    //----- Private implementation details

    private void waitForOpenToComplete() throws ClientException {
        if (!openFuture.isComplete() || openFuture.isFailed()) {
            try {
                openFuture.get();
            } catch (ExecutionException | InterruptedException e) {
                Thread.interrupted();
                if (failureCause.get() != null) {
                    throw failureCause.get();
                } else {
                    throw ClientExceptionSupport.createNonFatalOrPassthrough(e.getCause());
                }
            }
        }
    }

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
