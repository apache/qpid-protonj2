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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.ReceiveContext;
import org.apache.qpid.protonj2.client.ReceiveContextOptions;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.Source;
import org.apache.qpid.protonj2.client.Target;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceRemotelyClosedException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.client.util.FifoDeliveryQueue;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.types.messaging.Outcome;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientReceiver implements Receiver {

    private static final Logger LOG = LoggerFactory.getLogger(ClientReceiver.class);

    private static final AtomicIntegerFieldUpdater<ClientReceiver> CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientReceiver.class, "closed");

    private final ClientFuture<Receiver> openFuture;
    private final ClientFuture<Receiver> closeFuture;
    private ClientFuture<Receiver> drainingFuture;

    private final ReceiverOptions options;
    private final ClientSession session;
    private final org.apache.qpid.protonj2.engine.Receiver protonReceiver;
    private final ScheduledExecutorService executor;
    private final String receiverId;
    private final FifoDeliveryQueue messageQueue;
    private volatile int closed;
    private ClientException failureCause;

    private volatile Source remoteSource;
    private volatile Target remoteTarget;

    public ClientReceiver(ClientSession session, ReceiverOptions options, String receiverId, org.apache.qpid.protonj2.engine.Receiver receiver) {
        this.options = options;
        this.session = session;
        this.receiverId = receiverId;
        this.executor = session.getScheduler();
        this.openFuture = session.getFutureFactory().createFuture();
        this.closeFuture = session.getFutureFactory().createFuture();
        this.protonReceiver = receiver;

        // Ensure that the receiver can provide a link back to this object.
        protonReceiver.setLinkedResource(this);

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

    @Override
    public Future<Receiver> openFuture() {
        return openFuture;
    }

    @Override
    public Delivery receive() throws ClientException {
        return receive(-1, TimeUnit.MILLISECONDS);
    }

    @Override
    public Delivery receive(long timeout, TimeUnit units) throws ClientException {
        checkClosedOrFailed();

        try {
            ClientDelivery delivery = messageQueue.dequeue(units.toMillis(timeout));
            if (delivery != null) {
                if (options.autoAccept()) {
                    delivery.disposition(org.apache.qpid.protonj2.client.DeliveryState.accepted(), options.autoSettle());
                } else {
                    asyncReplenishCreditIfNeeded();
                }
            } else {
                checkClosedOrFailed();
            }

            return delivery;
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new ClientException("Receive wait interrupted", e);
        }
    }

    @Override
    public Delivery tryReceive() throws ClientException {
        checkClosedOrFailed();

        Delivery delivery = messageQueue.dequeueNoWait();
        if (delivery != null) {
            if (options.autoAccept()) {
                delivery.disposition(org.apache.qpid.protonj2.client.DeliveryState.accepted(), options.autoSettle());
            } else {
                asyncReplenishCreditIfNeeded();
            }
        } else {
            checkClosedOrFailed();
        }

        return delivery;
    }

    @Override
    public ReceiveContext openReceiveContext(ReceiveContextOptions options) {
        return new ClientReceiveContext(this, options);
    }

    private void asyncReplenishCreditIfNeeded() {
        int creditWindow = options.creditWindow();
        if (creditWindow > 0) {
            executor.execute(() -> replenishCreditIfNeeded());
        }
    }

    private void replenishCreditIfNeeded() {
        int creditWindow = options.creditWindow();
        if (creditWindow > 0) {
            int currentCredit = protonReceiver.getCredit();
            if (currentCredit <= creditWindow * 0.5) {
                int potentialPrefetch = currentCredit + messageQueue.size();

                if (potentialPrefetch <= creditWindow * 0.7) {
                    int additionalCredit = creditWindow - potentialPrefetch;

                    LOG.trace("Consumer granting additional credit: {}", additionalCredit);
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
    public ClientFuture<Receiver> close() {
        return doCloseOrDetach(true, null);
    }

    @Override
    public ClientFuture<Receiver> close(ErrorCondition error) {
        Objects.requireNonNull(error, "Error Condition cannot be null");

        return doCloseOrDetach(true, error);
    }

    @Override
    public ClientFuture<Receiver> detach() {
        return doCloseOrDetach(false, null);
    }

    @Override
    public ClientFuture<Receiver> detach(ErrorCondition error) {
        Objects.requireNonNull(error, "The provided Error Condition cannot be null");

        return doCloseOrDetach(false, error);
    }

    private ClientFuture<Receiver> doCloseOrDetach(boolean close, ErrorCondition error) {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            executor.execute(() -> {
                if (protonReceiver.isLocallyOpen()) {
                    try {
                        protonReceiver.setCondition(ClientErrorCondition.asProtonErrorCondition(error));

                        if (close) {
                            protonReceiver.close();
                        } else {
                            protonReceiver.detach();
                        }
                    } catch (Throwable ignore) {
                        closeFuture.complete(this);
                    }
                }
            });
        }

        return closeFuture;
    }

    @Override
    public long prefetchedCount() {
        return messageQueue.size();
    }

    @Override
    public Receiver addCredit(int credits) throws ClientException {
        checkClosedOrFailed();
        ClientFuture<Receiver> creditAdded = session.getFutureFactory().createFuture();

        executor.execute(() -> {
            checkClosedOrFailed(creditAdded);

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
        });

        return session.request(this, creditAdded);
    }

    @Override
    public Future<Receiver> drain() throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<Receiver> drainComplete = session.getFutureFactory().createFuture();

        executor.execute(() -> {
            checkClosedOrFailed(drainComplete);

            if (protonReceiver.isDraining()) {
                drainComplete.failed(new ClientException("Already draining"));
                return;
            }

            if (protonReceiver.getCredit() == 0) {
                drainComplete.complete(this);
                return;
            }

            try {
                if (protonReceiver.drain()) {
                    drainingFuture = drainComplete;
                } else {
                    drainComplete.complete(this);
                }
            } catch (Exception ex) {
                drainComplete.failed(ClientExceptionSupport.createNonFatalOrPassthrough(ex));
            }
        });

        return drainComplete;
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

    void disposition(IncomingDelivery delivery, DeliveryState state, boolean settled) throws ClientException {
        checkClosedOrFailed();
        executor.execute(() -> {
            if (session.getTransactionContext().isInTransaction()) {
                delivery.disposition(session.getTransactionContext().enlistAcknowledgeInCurrentTransaction(this, (Outcome) state), true);
            } else {
                delivery.disposition(state, settled);
            }

            replenishCreditIfNeeded();
        });
    }

    ClientReceiver open() {
        protonReceiver.localOpenHandler(this::handleLocalOpen)
                      .localCloseHandler(this::handleLocalCloseOrDetach)
                      .localDetachHandler(this::handleLocalCloseOrDetach)
                      .openHandler(this::handleRemoteOpen)
                      .closeHandler(this::handleRemoteCloseOrDetach)
                      .detachHandler(this::handleRemoteCloseOrDetach)
                      .parentEndpointClosedHandler(this::handleParentEndpointClosed)
                      .deliveryStateUpdatedHandler(this::handleDeliveryRemotelyUpdated)
                      .deliveryReadHandler(this::handleDeliveryReceived)
                      .creditStateUpdateHandler(this::handleReceiverCreditUpdated)
                      .engineShutdownHandler(this::handleEngineShutdown)
                      .open();

        return this;
    }

    void setFailureCause(ClientException failureCause) {
        this.failureCause = failureCause;
    }

    ClientException getFailureCause() {
        if (failureCause == null) {
            return session.getFailureCause();
        } else {
            return failureCause;
        }
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

    //----- Handlers for proton receiver events

    private void handleLocalOpen(org.apache.qpid.protonj2.engine.Receiver receiver) {
        if (options.openTimeout() > 0) {
            executor.schedule(() -> {
                if (!openFuture.isDone()) {
                    immediateLinkShutdown(new ClientOperationTimedOutException("Receiver open timed out waiting for remote to respond"));
                }
            }, options.openTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    private void handleLocalCloseOrDetach(org.apache.qpid.protonj2.engine.Receiver receiver) {
        messageQueue.stop();  // Ensure blocked receivers are all unblocked.

        // If not yet remotely closed we only wait for a remote close if the engine isn't
        // already failed and we have successfully opened the sender without a timeout.
        if (!receiver.getEngine().isShutdown() && failureCause == null && receiver.isRemotelyOpen()) {
            final long timeout = options.closeTimeout();

            if (timeout > 0) {
                session.scheduleRequestTimeout(closeFuture, timeout, () ->
                new ClientOperationTimedOutException("receiver close timed out waiting for remote to respond"));
            }
        } else {
            immediateLinkShutdown(failureCause);
        }
    }

    private void handleRemoteOpen(org.apache.qpid.protonj2.engine.Receiver receiver) {
        // Check for deferred close pending and hold completion if so
        if (receiver.getRemoteSource() != null) {
            remoteSource = new RemoteSource(receiver.getRemoteSource());

            if (receiver.getRemoteTarget() != null) {
                remoteTarget = new RemoteTarget(receiver.getRemoteTarget());
            }

            replenishCreditIfNeeded();

            openFuture.complete(this);
            LOG.trace("Receiver opened successfully: {}", receiverId);
        } else {
            LOG.debug("Receiver opened but remote signalled close is pending: {}", receiverId);
        }
    }

    private void handleRemoteCloseOrDetach(org.apache.qpid.protonj2.engine.Receiver receiver) {
        if (receiver.isLocallyOpen()) {
            immediateLinkShutdown(ClientExceptionSupport.convertToLinkClosedException(
                receiver.getRemoteCondition(), "Receiver remotely closed without explanation from the remote"));
        } else {
            immediateLinkShutdown(failureCause);
        }
    }

    private void handleParentEndpointClosed(org.apache.qpid.protonj2.engine.Receiver receiver) {
        final ClientException failureCause;

        if (receiver.getConnection().getRemoteCondition() != null) {
            failureCause = ClientExceptionSupport.convertToConnectionClosedException(receiver.getConnection().getRemoteCondition());
        } else if (receiver.getSession().getRemoteCondition() != null) {
            failureCause = ClientExceptionSupport.convertToSessionClosedException(receiver.getSession().getRemoteCondition());
        } else if (receiver.getEngine().failureCause() != null) {
            failureCause = ClientExceptionSupport.convertToConnectionClosedException(receiver.getEngine().failureCause());
        } else if (!isClosed()) {
            failureCause = new ClientResourceRemotelyClosedException("Remote closed without a specific error condition");
        } else {
            failureCause = null;
        }

        immediateLinkShutdown(failureCause);
    }

    private void handleEngineShutdown(Engine engine) {
        final Connection connection = engine.connection();

        final ClientException failureCause;

        if (connection.getRemoteCondition() != null) {
            failureCause = ClientExceptionSupport.convertToConnectionClosedException(connection.getRemoteCondition());
        } else if (engine.failureCause() != null) {
            failureCause = ClientExceptionSupport.convertToConnectionClosedException(engine.failureCause());
        } else if (!isClosed()) {
            failureCause = new ClientConnectionRemotelyClosedException("Remote closed without a specific error condition");
        } else {
            failureCause = null;
        }

        immediateLinkShutdown(failureCause);
    }

    private void handleDeliveryReceived(IncomingDelivery delivery) {
        LOG.trace("Delivery data was received: {}", delivery);

        if (delivery.getDefaultDeliveryState() != null) {
            delivery.setDefaultDeliveryState(Released.getInstance());
        }

        if (delivery.isAborted()) {
            delivery.settle();
            replenishCreditIfNeeded();
        } else if (!delivery.isPartial()) {
            LOG.trace("{} has incoming Message(s).", this);
            messageQueue.enqueue(new ClientDelivery(this, delivery));
        }
    }

    private void handleDeliveryRemotelyUpdated(IncomingDelivery delivery) {
        LOG.trace("Delivery was updated: {}", delivery);
    }

    private void handleReceiverCreditUpdated(org.apache.qpid.protonj2.engine.Receiver receiver) {
        LOG.trace("Receiver credit update by remote: {}", receiver);

        if (drainingFuture != null) {
            if (receiver.getCredit() == 0) {
                drainingFuture.complete(this);
            }
        }
    }

    //----- Private implementation details

    private void waitForOpenToComplete() throws ClientException {
        if (!openFuture.isComplete() || openFuture.isFailed()) {
            try {
                openFuture.get();
            } catch (ExecutionException | InterruptedException e) {
                Thread.interrupted();
                if (failureCause != null) {
                    throw failureCause;
                } else {
                    throw ClientExceptionSupport.createNonFatalOrPassthrough(e.getCause());
                }
            }
        }
    }

    private void checkClosedOrFailed(ClientFuture<?> request) {
        if (isClosed()) {
            request.failed(new ClientIllegalStateException("The Receiver was explicity closed", failureCause));
        } else if (failureCause != null) {
            request.failed(failureCause);
        }
    }

    protected void checkClosedOrFailed() throws ClientException {
        if (isClosed()) {
            throw new ClientIllegalStateException("The Receiver was explicity closed", failureCause);
        } else if (failureCause != null) {
            throw failureCause;
        }
    }

    private void immediateLinkShutdown(ClientException failureCause) {
        if (this.failureCause == null) {
            this.failureCause = failureCause;
        }

        try {
            if (protonReceiver.isRemotelyDetached()) {
                protonReceiver.detach();
            } else {
                protonReceiver.close();
            }
        } catch (Exception ignore) {
        }

        if (failureCause != null) {
            openFuture.failed(failureCause);
            if (drainingFuture != null) {
                drainingFuture.failed(failureCause);
            }
        } else {
            openFuture.complete(this);
            if (drainingFuture != null) {
                drainingFuture.failed(new ClientResourceRemotelyClosedException("The Receiver has been closed"));
            }
        }

        closeFuture.complete(this);
    }
}
