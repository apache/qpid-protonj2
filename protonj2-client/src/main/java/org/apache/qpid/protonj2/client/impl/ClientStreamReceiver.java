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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Source;
import org.apache.qpid.protonj2.client.StreamDelivery;
import org.apache.qpid.protonj2.client.StreamReceiver;
import org.apache.qpid.protonj2.client.StreamReceiverOptions;
import org.apache.qpid.protonj2.client.Target;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceRemotelyClosedException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ClientStreamReceiver implements StreamReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(ClientReceiver.class);

    private static final AtomicIntegerFieldUpdater<ClientStreamReceiver> CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientStreamReceiver.class, "closed");

    private final ClientFuture<Receiver> openFuture;
    private final ClientFuture<Receiver> closeFuture;
    private ClientFuture<Receiver> drainingFuture;

    private final StreamReceiverOptions options;
    private final ClientSession session;
    private final org.apache.qpid.protonj2.engine.Receiver protonReceiver;
    private final ScheduledExecutorService executor;
    private final String receiverId;
    private final Map<ClientFuture<StreamDelivery>, ScheduledFuture<?>> receiveRequests = new LinkedHashMap<>();

    private volatile int closed;
    private ClientException failureCause;
    private volatile Source remoteSource;
    private volatile Target remoteTarget;

    public ClientStreamReceiver(ClientSession session, StreamReceiverOptions options, String receiverId, org.apache.qpid.protonj2.engine.Receiver receiver) {
        this.options = options;
        this.session = session;
        this.receiverId = receiverId;
        this.executor = session.getScheduler();
        this.openFuture = session.getFutureFactory().createFuture();
        this.closeFuture = session.getFutureFactory().createFuture();
        this.protonReceiver = receiver.setLinkedResource(this);

        if (options.creditWindow() > 0) {
            protonReceiver.addCredit(options.creditWindow());
        }
    }

    @Override
    public ClientInstance client() {
        return session.client();
    }

    @Override
    public ClientConnection connection() {
        return session.connection();
    }

    @Override
    public ClientSession session() {
        return session;
    }

    @Override
    public ClientFuture<Receiver> openFuture() {
        return openFuture;
    }

    @Override
    public void close() {
        try {
            doCloseOrDetach(true, null).get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.interrupted();
        }
    }

    @Override
    public void close(ErrorCondition error) {
        Objects.requireNonNull(error, "Error Condition cannot be null");

        try {
            doCloseOrDetach(true, error).get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.interrupted();
        }
    }

    @Override
    public void detach() {
        try {
            doCloseOrDetach(false, null).get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.interrupted();
        }
    }

    @Override
    public void detach(ErrorCondition error) {
        Objects.requireNonNull(error, "Error Condition cannot be null");

        try {
            doCloseOrDetach(false, error).get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.interrupted();
        }
    }

    @Override
    public ClientFuture<Receiver> closeAsync() {
        return doCloseOrDetach(true, null);
    }

    @Override
    public ClientFuture<Receiver> closeAsync(ErrorCondition error) {
        Objects.requireNonNull(error, "Error Condition cannot be null");

        return doCloseOrDetach(true, error);
    }

    @Override
    public ClientFuture<Receiver> detachAsync() {
        return doCloseOrDetach(false, null);
    }

    @Override
    public ClientFuture<Receiver> detachAsync(ErrorCondition error) {
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
    public StreamDelivery receive() throws ClientException {
        return receive(-1, TimeUnit.MILLISECONDS);
    }

    @Override
    public StreamDelivery receive(long timeout, TimeUnit unit) throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<StreamDelivery> receive = session.getFutureFactory().createFuture();

        executor.execute(() -> {
            checkClosedOrFailed(receive);
            IncomingDelivery delivery = null;

            // Scan for an unsettled delivery that isn't yet assigned to a client delivery
            // either it is a complete delivery or the initial stage of the next incoming
            for (IncomingDelivery unsettled : protonReceiver.unsettled()) {
                if (unsettled.getLinkedResource() == null) {
                    delivery = unsettled;
                    break;
                }
            }

            if (delivery == null) {
                if (timeout == 0) {
                    receive.complete(null);
                } else {
                    final ScheduledFuture<?> timeoutFuture;

                    if (timeout > 0) {
                        timeoutFuture = session.getScheduler().schedule(() -> {
                            receiveRequests.remove(receive);
                            receive.complete(null); // Timed receive returns null on failed wait.
                        }, timeout, unit);
                    } else {
                        timeoutFuture = null;
                    }

                    receiveRequests.put(receive, timeoutFuture);
                }
            } else {
                receive.complete(new ClientStreamDelivery(this, delivery));
            }
        });

        return session.request(this, receive);
    }

    @Override
    public StreamDelivery tryReceive() throws ClientException {
        checkClosedOrFailed();
        return receive(0, TimeUnit.MILLISECONDS);
    }

    @Override
    public StreamReceiver addCredit(int credits) throws ClientException {
        checkClosedOrFailed();
        ClientFuture<StreamReceiver> creditAdded = session.getFutureFactory().createFuture();

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
    public long queuedDeliveries() throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<Integer> request = session.getFutureFactory().createFuture();

        executor.execute(() -> {
            checkClosedOrFailed(request);

            int queued = 0;

            // Scan for an unsettled delivery that isn't yet assigned to a client delivery
            // either it is a complete delivery or the initial stage of the next incoming
            for (IncomingDelivery unsettled : protonReceiver.unsettled()) {
                if (unsettled.getLinkedResource() == null) {
                    queued++;
                }
            }

            request.complete(queued);
        });

        return session.request(this, request);
    }

    //----- Internal API for the ClientReceiver and other Client objects

    ClientStreamReceiver open() {
        protonReceiver.localOpenHandler(this::handleLocalOpen)
                      .localCloseHandler(this::handleLocalCloseOrDetach)
                      .localDetachHandler(this::handleLocalCloseOrDetach)
                      .openHandler(this::handleRemoteOpen)
                      .closeHandler(this::handleRemoteCloseOrDetach)
                      .detachHandler(this::handleRemoteCloseOrDetach)
                      .parentEndpointClosedHandler(this::handleParentEndpointClosed)
                      .deliveryStateUpdatedHandler(this::handleDeliveryStateRemotelyUpdated)
                      .deliveryReadHandler(this::handleDeliveryRead)
                      .deliveryAbortedHandler(this::handleDeliveryAborted)
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

    StreamReceiverOptions receiverOptions() {
        return options;
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
            remoteSource = new ClientRemoteSource(receiver.getRemoteSource());

            if (receiver.getRemoteTarget() != null) {
                remoteTarget = new ClientRemoteTarget(receiver.getRemoteTarget());
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
        final org.apache.qpid.protonj2.engine.Connection connection = engine.connection();

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

    private void handleDeliveryRead(IncomingDelivery delivery) {
        LOG.trace("Delivery data was received: {}", delivery);
        if (delivery.getDefaultDeliveryState() == null) {
            delivery.setDefaultDeliveryState(Released.getInstance());
        }

        if (delivery.getLinkedResource() == null) {
            // New delivery that can be sent to a waiting receive caller
            if (!receiveRequests.isEmpty()) {
                Iterator<Entry<ClientFuture<StreamDelivery>, ScheduledFuture<?>>> entries =
                    receiveRequests.entrySet().iterator();

                Entry<ClientFuture<StreamDelivery>, ScheduledFuture<?>> entry = entries.next();
                if (entry.getValue() != null) {
                    entry.getValue().cancel(false);
                }

                try {
                    entry.getKey().complete(new ClientStreamDelivery(this, delivery));
                } finally {
                    entries.remove();
                }
            }
        }
    }

    private void handleDeliveryAborted(IncomingDelivery delivery) {
        LOG.trace("Delivery data was aborted: {}", delivery);
        delivery.settle();
        replenishCreditIfNeeded();
    }

    private void handleDeliveryStateRemotelyUpdated(IncomingDelivery delivery) {
        LOG.trace("Delivery remote state was updated: {}", delivery);
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

    void disposition(IncomingDelivery delivery, DeliveryState state, boolean settle) throws ClientException {
        checkClosedOrFailed();
        asyncApplyDisposition(delivery, state, settle);
    }

    private void asyncApplyDisposition(IncomingDelivery delivery, DeliveryState state, boolean settle) throws ClientException {
        executor.execute(() -> {
            session.getTransactionContext().disposition(delivery, state, settle);
            replenishCreditIfNeeded();
        });
    }

    private void replenishCreditIfNeeded() {
        int creditWindow = options.creditWindow();
        if (creditWindow > 0) {
            int currentCredit = protonReceiver.getCredit();
            if (currentCredit <= creditWindow * 0.5) {
                int potentialPrefetch = currentCredit + protonReceiver.unsettled().size();

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
            // Ignore
        } finally {
            session.closeAsync();
        }

        receiveRequests.forEach((future, timeout) -> {
            if (timeout != null) {
                timeout.cancel(false);
            }

            if (failureCause != null) {
                future.failed(failureCause);
            } else {
                future.failed(new ClientResourceRemotelyClosedException("The Stream Receiver has closed"));
            }
        });

        protonReceiver.unsettled().forEach((delivery) -> {
            if (delivery.getLinkedResource() != null) {
                try {
                    delivery.getLinkedResource(ClientStreamDelivery.class).handleReceiverClosed(this);
                } catch (Exception ex) {}
            }
        });

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
