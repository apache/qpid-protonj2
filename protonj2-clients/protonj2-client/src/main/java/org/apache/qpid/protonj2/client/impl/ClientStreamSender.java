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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.DeliveryMode;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.Source;
import org.apache.qpid.protonj2.client.StreamSender;
import org.apache.qpid.protonj2.client.StreamSenderOptions;
import org.apache.qpid.protonj2.client.Target;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientSendTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientUnsupportedOperationException;
import org.apache.qpid.protonj2.client.futures.AsyncResult;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.LinkState;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientStreamSender implements StreamSender {

    private static final Logger LOG = LoggerFactory.getLogger(ClientStreamSender.class);

    private static final AtomicIntegerFieldUpdater<ClientStreamSender> CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientStreamSender.class, "closed");

    private final ClientFuture<Sender> openFuture;
    private final ClientFuture<Sender> closeFuture;

    private final StreamSenderOptions options;
    private final ClientSession session;
    private final org.apache.qpid.protonj2.engine.Sender protonSender;
    private final ScheduledExecutorService executor;
    private final String senderId;

    private volatile Source remoteSource;
    private volatile Target remoteTarget;

    private InFlightSend blockedOnCredit;
    private volatile int closed;
    private ClientException failureCause;

    public ClientStreamSender(ClientSession session, StreamSenderOptions options, String senderId, org.apache.qpid.protonj2.engine.Sender protonSender) {
        this.options = new StreamSenderOptions(options);
        this.session = session;
        this.senderId = senderId;
        this.executor = session.getScheduler();
        this.openFuture = session.getFutureFactory().createFuture();
        this.closeFuture = session.getFutureFactory().createFuture();
        this.protonSender = protonSender.setLinkedResource(this);
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
    public String address() throws ClientException {
        final org.apache.qpid.protonj2.types.messaging.Target target;
        if (isDynamic()) {
            waitForOpenToComplete();
            target = protonSender.getRemoteTarget();
        } else {
            target = protonSender.getTarget();
        }

        if (target != null) {
            return target.getAddress();
        } else {
            return null;
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
    public Map<String, Object> properties() throws ClientException {
        waitForOpenToComplete();
        return ClientConversionSupport.toStringKeyedMap(protonSender.getRemoteProperties());
    }

    @Override
    public String[] offeredCapabilities() throws ClientException {
        waitForOpenToComplete();
        return ClientConversionSupport.toStringArray(protonSender.getRemoteOfferedCapabilities());
    }

    @Override
    public String[] desiredCapabilities() throws ClientException {
        waitForOpenToComplete();
        return ClientConversionSupport.toStringArray(protonSender.getRemoteDesiredCapabilities());
    }

    @Override
    public Future<Sender> openFuture() {
        return openFuture;
    }

    @Override
    public ClientFuture<Sender> close() {
        return doCloseOrDetach(true, null);
    }

    @Override
    public ClientFuture<Sender> close(ErrorCondition error) {
        Objects.requireNonNull(error, "Error Condition cannot be null");

        return doCloseOrDetach(true, error);
    }

    @Override
    public ClientFuture<Sender> detach() {
        return doCloseOrDetach(false, null);
    }

    @Override
    public ClientFuture<Sender> detach(ErrorCondition error) {
        Objects.requireNonNull(error, "Error Condition cannot be null");

        return doCloseOrDetach(false, error);
    }

    private ClientFuture<Sender> doCloseOrDetach(boolean close, ErrorCondition error) {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            executor.execute(() -> {
                if (protonSender.isLocallyOpen()) {
                    try {
                        protonSender.setCondition(ClientErrorCondition.asProtonErrorCondition(error));

                        if (close) {
                            protonSender.close();
                        } else {
                            protonSender.detach();
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
    public Tracker send(Message<?> message) throws ClientException {
        return send(message, null);
    }

    @Override
    public Tracker send(Message<?> message, Map<String, Object> deliveryAnnotations) throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<Tracker> tracker = session.getFutureFactory().createFuture();

        executor.execute(() -> {
            if (protonSender.current() != null) {
                // TODO: Add to blocked list and send after ?
                tracker.failed(new ClientIllegalStateException(
                    "Cannot perform a send while a streaming send is in progress"));
            } else {
                // TODO:
            }
        });

        return session.request(this, tracker);
    }

    @Override
    public Tracker trySend(Message<?> message) throws ClientException {
        return trySend(message, null);
    }

    @Override
    public Tracker trySend(Message<?> message, Map<String, Object> deliveryAnnotations) throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<Tracker> tracker = session.getFutureFactory().createFuture();

        executor.execute(() -> {
            if (protonSender.current() != null) {
                tracker.complete(null);
            } else {
                // TODO:
            }
        });

        return session.request(this, tracker);
    }

    @Override
    public ClientStreamSenderMessage beginMessage() throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<ClientStreamSenderMessage> tracker = session.getFutureFactory().createFuture();

        executor.execute(() -> {
            if (protonSender.current() != null) {
                tracker.failed(new ClientIllegalStateException(
                    "Cannot initiate a new streaming send until the previous one is complete"));
            } else {
                tracker.complete(new ClientStreamSenderMessage(this, protonSender.next()));
            }
        });

        return session.request(this, tracker);
    }

    //----- Internal API

    void disposition(OutgoingDelivery delivery, DeliveryState state, boolean settled) throws ClientException {
        checkClosedOrFailed();
        executor.execute(() -> {
            delivery.disposition(state, settled);
        });
    }

    void abort(OutgoingDelivery delivery) throws ClientException {
        checkClosedOrFailed();
        executor.execute(() -> {
            delivery.abort();
        });
    }

    void complete(OutgoingDelivery delivery) throws ClientException {
        checkClosedOrFailed();
        executor.execute(() -> {
            delivery.streamBytes(null, true);
        });
    }

    ClientStreamSender open() {
        protonSender.localOpenHandler(this::handleLocalOpen)
                    .localCloseHandler(this::handleLocalCloseOrDetach)
                    .localDetachHandler(this::handleLocalCloseOrDetach)
                    .openHandler(this::handleRemoteOpen)
                    .closeHandler(this::handleRemoteCloseOrDetach)
                    .detachHandler(this::handleRemoteCloseOrDetach)
                    .parentEndpointClosedHandler(this::handleParentEndpointClosed)
                    .deliveryStateUpdatedHandler(this::handleDeliveryUpdated)
                    .creditStateUpdateHandler(this::handleCreditStateUpdated)
                    .engineShutdownHandler(this::handleEngineShutdown)
                    .open();

        return this;
    }

    void setFailureCause(ClientException failureCause) {
        this.failureCause = failureCause;
    }

    org.apache.qpid.protonj2.engine.Sender getProtonSender() {
        return protonSender;
    }

    ClientException getFailureCause() {
        if (failureCause == null) {
            return session.getFailureCause();
        } else {
            return failureCause;
        }
    }

    String getId() {
        return senderId;
    }

    boolean isClosed() {
        return closed > 0;
    }

    boolean isAnonymous() {
        return protonSender.<org.apache.qpid.protonj2.types.messaging.Target>getTarget().getAddress() == null;
    }

    boolean isDynamic() {
        return protonSender.getTarget() != null && protonSender.<org.apache.qpid.protonj2.types.messaging.Target>getTarget().isDynamic();
    }

    StreamSenderOptions options() {
        return this.options;
    }

    void sendMessage(ClientStreamSenderMessage context, AdvancedMessage<?> message) throws ClientException {
        final ClientFuture<Void> operation = session.getFutureFactory().createFuture();
        final ProtonBuffer buffer = message.encode(null);

        executor.execute(() -> {
            checkClosedOrFailed(operation);

            if (protonSender.isSendable()) {
                try {
                    assumeSendableAndSend(context, buffer, operation);
                } catch (Exception ex) {
                    operation.failed(ClientExceptionSupport.createNonFatalOrPassthrough(ex));
                }
            } else {
                final InFlightSend send = new InFlightSend(context, buffer, operation);
                if (options.sendTimeout() > 0) {
                    send.timeout = session.scheduleRequestTimeout(
                        operation, options.sendTimeout(), () -> send.createSendTimedOutException());
                }

                blockedOnCredit = send;
            }
        });

        session.request(this, operation);
    }

    private void assumeSendableAndSend(ClientStreamSenderMessage message, ProtonBuffer buffer, AsyncResult<Void> request) throws ClientException {
        final OutgoingDelivery delivery = protonSender.current().setMessageFormat(message.messageFormat());

        final ClientStreamTracker tracker = message.tracker();

        if (session.getTransactionContext().isInTransaction()) {
            if (session.getTransactionContext().isTransactionInDoubt()) {
                tracker.settlementFuture().complete(tracker);
                request.complete(null);
                return;
            } else {
                delivery.disposition(session.getTransactionContext().enlistSendInCurrentTransaction(this, delivery),
                    protonSender.getSenderSettleMode() == SenderSettleMode.SETTLED || delivery.isSettled());
            }
        } else {
            delivery.disposition(delivery.getState(), protonSender.getSenderSettleMode() == SenderSettleMode.SETTLED || delivery.isSettled());
        }

        if (delivery.isSettled()) {
            // Remote will not update this delivery so mark as acknowledged now.
            tracker.settlementFuture().complete(tracker);
        }

        if (message.aborted()) {
            delivery.abort();
            request.complete(null);
        } else {
            // For now we only complete the request after the IO operation when the delivery mode is
            // something other than DeliveryMode.AT_MOST_ONCE.  We should think about other options to
            // allow this under other modes given the tracker can communicate errors if the eventual
            // IO operation does indeed fail otherwise most modes suffer a more significant performance
            // hit waiting on the IO operation to complete.
            if (options.deliveryMode() == DeliveryMode.AT_MOST_ONCE) {
                request.complete(null);
                delivery.streamBytes(buffer, message.completed());
            } else {
                delivery.streamBytes(buffer, message.completed());
                request.complete(null);
            }

            if (buffer.isReadable()) {
                blockedOnCredit = blockedOnCredit != null ?
                    blockedOnCredit : new InFlightSend(message, buffer, (ClientFuture<Void>) request);
            } else {
                blockedOnCredit = null;
            }
        }
    }

    protected void checkClosedOrFailed(ClientFuture<?> request) {
        if (isClosed()) {
            request.failed(new ClientIllegalStateException("The Sender was explicity closed", failureCause));
        } else if (failureCause != null) {
            request.failed(failureCause);
        }
    }

    protected void checkClosedOrFailed() throws ClientException {
        if (isClosed()) {
            throw new ClientIllegalStateException("The Sender was explicity closed", failureCause);
        } else if (failureCause != null) {
            throw failureCause;
        }
    }

    private void immediateLinkShutdown(ClientException failureCause) {
        if (this.failureCause == null) {
            this.failureCause = failureCause;
        }

        try {
            if (protonSender.isRemotelyDetached()) {
                protonSender.detach();
            } else {
                protonSender.close();
            }
        } catch (Throwable ignore) {
        }

        // Cancel any blocked send passing an appropriate error to the future
        if (blockedOnCredit != null) {
            if (failureCause != null) {
                blockedOnCredit.operation.failed(failureCause);
            } else {
                blockedOnCredit.operation.failed(new ClientResourceRemotelyClosedException("The sender link has closed"));
            }
        }

        protonSender.unsettled().forEach((delivery) -> {
            try {
                ClientStreamTracker tracker = delivery.getLinkedResource();
                if (failureCause != null) {
                    tracker.settlementFuture().failed(failureCause);
                } else {
                    tracker.settlementFuture().failed(new ClientResourceRemotelyClosedException("The sender link has closed"));
                }
            } catch (Exception e) {
            }
        });

        if (failureCause != null) {
            openFuture.failed(failureCause);
        } else {
            openFuture.complete(this);
        }

        closeFuture.complete(this);
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

    //----- Send Result Tracker used for send blocked on credit

    private class InFlightSend implements AsyncResult<Void> {

        private final ClientStreamSenderMessage context;
        private final ProtonBuffer encodedMessage;
        private final ClientFuture<Void> operation;

        private ScheduledFuture<?> timeout;

        public InFlightSend(ClientStreamSenderMessage context, ProtonBuffer encodedMessage, ClientFuture<Void> operation) {
            this.encodedMessage = encodedMessage;
            this.context = context;
            this.operation = operation;
        }

        @Override
        public void failed(ClientException result) {
            if (timeout != null) {
                timeout.cancel(true);
            }

            operation.failed(result);
        }

        @Override
        public void complete(Void result) {
            if (timeout != null) {
                timeout.cancel(true);
            }

            operation.complete(null);
        }

        @Override
        public boolean isComplete() {
            return operation.isDone();
        }

        public ClientException createSendTimedOutException() {
            return new ClientSendTimedOutException("Timed out waiting for credit to send");
        }
    }

    //----- Handlers for proton receiver events

    private void handleLocalOpen(org.apache.qpid.protonj2.engine.Sender sender) {
        if (options.openTimeout() > 0) {
            executor.schedule(() -> {
                if (!openFuture.isDone()) {
                    immediateLinkShutdown(new ClientOperationTimedOutException("Sender open timed out waiting for remote to respond"));
                }
            }, options.openTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    private void handleLocalCloseOrDetach(org.apache.qpid.protonj2.engine.Sender sender) {
        // If not yet remotely closed we only wait for a remote close if the engine isn't
        // already failed and we have successfully opened the sender without a timeout.
        if (!sender.getEngine().isShutdown() && failureCause == null && sender.isRemotelyOpen()) {
            final long timeout = options.closeTimeout();

            if (timeout > 0) {
                session.scheduleRequestTimeout(closeFuture, timeout, () ->
                    new ClientOperationTimedOutException("Sender close timed out waiting for remote to respond"));
            }
        } else {
            immediateLinkShutdown(failureCause);
        }
    }

    private void handleParentEndpointClosed(org.apache.qpid.protonj2.engine.Sender sender) {
        final ClientException failureCause;

        if (sender.getConnection().getRemoteCondition() != null) {
            failureCause = ClientExceptionSupport.convertToConnectionClosedException(sender.getConnection().getRemoteCondition());
        } else if (sender.getSession().getRemoteCondition() != null) {
            failureCause = ClientExceptionSupport.convertToSessionClosedException(sender.getSession().getRemoteCondition());
        } else if (sender.getEngine().failureCause() != null) {
            failureCause = ClientExceptionSupport.convertToConnectionClosedException(sender.getEngine().failureCause());
        } else if (!isClosed()) {
            failureCause = new ClientResourceRemotelyClosedException("Remote closed without a specific error condition");
        } else {
            failureCause = null;
        }

        immediateLinkShutdown(failureCause);
    }

    private void handleRemoteOpen(org.apache.qpid.protonj2.engine.Sender sender) {
        // Check for deferred close pending and hold completion if so
        if (sender.getRemoteTarget() != null) {
            remoteSource = new RemoteSource(sender.getRemoteSource());

            if (sender.getRemoteTarget() != null) {
                remoteTarget = new RemoteTarget(sender.getRemoteTarget());
            }

            openFuture.complete(this);
            LOG.trace("Sender opened successfully");
        } else {
            LOG.debug("Sender opened but remote signalled close is pending: ", sender);
        }
    }

    private void handleRemoteCloseOrDetach(org.apache.qpid.protonj2.engine.Sender sender) {
        if (sender.isLocallyOpen()) {
            immediateLinkShutdown(ClientExceptionSupport.convertToLinkClosedException(
                sender.getRemoteCondition(), "Sender remotely closed without explanation from the remote"));
        } else {
            immediateLinkShutdown(failureCause);
        }
    }

    private void handleCreditStateUpdated(org.apache.qpid.protonj2.engine.Sender sender) {
        if (sender.isSendable() && blockedOnCredit != null) {
            LOG.trace("Dispatching previously held send");
            try {
                assumeSendableAndSend(blockedOnCredit.context, blockedOnCredit.encodedMessage, blockedOnCredit);
            } catch (Exception ex) {
                blockedOnCredit.failed(ClientExceptionSupport.createNonFatalOrPassthrough(ex));
            }
        }

        if (sender.isDraining() && blockedOnCredit == null) {
            sender.drained();
        }
    }

    private void handleDeliveryUpdated(OutgoingDelivery delivery) {
        try {
            delivery.getLinkedResource(ClientStreamSenderMessage.class).tracker().processDeliveryUpdated(delivery);
        } catch (ClassCastException ccex) {
            LOG.debug("Sender received update on Delivery not linked to a Tracker: {}", delivery);
        }

        if (options.autoSettle() && delivery.isRemotelySettled()) {
            delivery.settle();
        }
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

    void handleAnonymousRelayNotSupported() {
        if (isAnonymous() && protonSender.getState() == LinkState.IDLE) {
            immediateLinkShutdown(new ClientUnsupportedOperationException("Anonymous relay support not available from this connection"));
        }
    }
}
