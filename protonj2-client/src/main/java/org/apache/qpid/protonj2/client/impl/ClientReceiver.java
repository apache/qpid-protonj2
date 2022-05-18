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

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.client.util.FifoDeliveryQueue;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client {@link Receiver} implementation.
 */
public final class ClientReceiver extends ClientReceiverLinkType<Receiver> implements Receiver {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final ReceiverOptions options;
    private final FifoDeliveryQueue deliveryQueue;

    ClientReceiver(ClientSession session, ReceiverOptions options, String receiverId, org.apache.qpid.protonj2.engine.Receiver receiver) {
        super(session, receiverId, options, receiver);

        this.options = options;

        if (options.creditWindow() > 0) {
            protonReceiver.addCredit(options.creditWindow());
        }

        deliveryQueue = new FifoDeliveryQueue(options.creditWindow());
        deliveryQueue.start();
    }

    @Override
    public Delivery receive() throws ClientException {
        return receive(-1, TimeUnit.MILLISECONDS);
    }

    @Override
    public Delivery receive(long timeout, TimeUnit units) throws ClientException {
        checkClosedOrFailed();

        try {
            ClientDelivery delivery = deliveryQueue.dequeue(units.toMillis(timeout));
            if (delivery != null) {
                if (options.autoAccept()) {
                    disposition(delivery.protonDelivery(), Accepted.getInstance(), options.autoSettle());
                } else if (options.creditWindow() > 0) {
                    executor.execute(() -> replenishCreditIfNeeded());
                }

                return delivery;
            }

            checkClosedOrFailed();

            return null;
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new ClientException("Receive wait interrupted", e);
        }
    }

    @Override
    public Delivery tryReceive() throws ClientException {
        checkClosedOrFailed();

        Delivery delivery = deliveryQueue.dequeueNoWait();
        if (delivery != null) {
            if (options.autoAccept()) {
                delivery.disposition(org.apache.qpid.protonj2.client.DeliveryState.accepted(), options.autoSettle());
            } else if (options.creditWindow() > 0) {
                executor.execute(() -> replenishCreditIfNeeded());
            }
        } else {
            checkClosedOrFailed();
        }

        return delivery;
    }

    @Override
    public long queuedDeliveries() {
        return deliveryQueue.size();
    }

    @Override
    public Receiver addCredit(int credits) throws ClientException {
        checkClosedOrFailed();
        ClientFuture<Receiver> creditAdded = session.getFutureFactory().createFuture();

        executor.execute(() -> {
            if (notClosedOrFailed(creditAdded)) {
                if (options.creditWindow() != 0) {
                    creditAdded.failed(new ClientIllegalStateException("Cannot add credit when a credit window has been configured"));
                } else if (protonReceiver.isDraining()) {
                    creditAdded.failed(new ClientIllegalStateException("Cannot add credit while a drain is pending"));
                } else {
                    try {
                        protonReceiver.addCredit(credits);
                        creditAdded.complete(this);
                    } catch (Exception ex) {
                        creditAdded.failed(ClientExceptionSupport.createNonFatalOrPassthrough(ex));
                    }
                }
            }
        });

        return session.request(this, creditAdded);
    }

    @Override
    public Future<Receiver> drain() throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<Receiver> drainComplete = session.getFutureFactory().createFuture();

        executor.execute(() -> {
            if (notClosedOrFailed(drainComplete)) {
                if (protonReceiver.isDraining()) {
                    drainComplete.failed(new ClientIllegalStateException("Receiver is already draining"));
                    return;
                }

                try {
                    if (protonReceiver.drain()) {
                        drainingFuture = drainComplete;
                        drainingTimeout = session.scheduleRequestTimeout(drainingFuture, options.drainTimeout(),
                            () -> new ClientOperationTimedOutException("Timed out waiting for remote to respond to drain request"));
                    } else {
                        drainComplete.complete(this);
                    }
                } catch (Exception ex) {
                    drainComplete.failed(ClientExceptionSupport.createNonFatalOrPassthrough(ex));
                }
            }
        });

        return drainComplete;
    }

    //----- Internal API for the ClientReceiver and other Client objects

    @Override
    protected Receiver self() {
        return this;
    }

    //----- Handlers for proton receiver events

    @Override
    protected void handleDeliveryRead(IncomingDelivery delivery) {
        LOG.trace("Delivery data was received: {}", delivery);

        if (delivery.getDefaultDeliveryState() == null) {
            delivery.setDefaultDeliveryState(Released.getInstance());
        }

        if (!delivery.isPartial()) {
            LOG.trace("{} has incoming Message(s).", this);
            deliveryQueue.enqueue(new ClientDelivery(this, delivery));
        } else {
            delivery.claimAvailableBytes();
        }
    }

    //----- Private implementation details

    @Override
    protected void replenishCreditIfNeeded() {
        int creditWindow = options.creditWindow();
        if (creditWindow > 0) {
            int currentCredit = protonReceiver.getCredit();
            if (currentCredit <= creditWindow * 0.5) {
                int potentialPrefetch = currentCredit + deliveryQueue.size();

                if (potentialPrefetch <= creditWindow * 0.7) {
                    int additionalCredit = creditWindow - potentialPrefetch;

                    LOG.trace("Receiver {} granting additional credit: {}", linkId, additionalCredit);
                    try {
                        protonReceiver.addCredit(additionalCredit);
                    } catch (Exception ex) {
                        LOG.debug("Error caught during credit top-up", ex);
                    }
                }
            }
        }
    }

    @Override
    protected void linkSpecificLocalCloseHandler() {
        deliveryQueue.stop();  // Ensure blocked receivers are all unblocked.
        deliveryQueue.clear();
    }

    @Override
    protected void recreateLinkForReconnect() {
        int previousCredit = protonReceiver.getCredit() + deliveryQueue.size();

        deliveryQueue.clear();  // Prefetched messages should be discarded.

        if (drainingFuture != null) {
            drainingFuture.complete(this);
            if (drainingTimeout != null) {
                drainingTimeout.cancel(false);
                drainingTimeout = null;
            }
        }

        protonReceiver.localCloseHandler(null);
        protonReceiver.localDetachHandler(null);
        protonReceiver.close();
        protonReceiver = ClientReceiverBuilder.recreateReceiver(session, protonReceiver, options);
        protonReceiver.setLinkedResource(this);
        protonReceiver.addCredit(previousCredit);
    }
}
