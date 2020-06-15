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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.OutgoingDelivery;
import org.apache.qpid.proton4j.engine.impl.ProtonDeliveryTagGenerator;
import org.apache.qpid.proton4j.types.transport.DeliveryState;
import org.apache.qpid.proton4j.types.transport.SenderSettleMode;
import org.messaginghub.amqperative.ErrorCondition;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.Source;
import org.messaginghub.amqperative.Target;
import org.messaginghub.amqperative.Tracker;
import org.messaginghub.amqperative.exceptions.ClientException;
import org.messaginghub.amqperative.exceptions.ClientOperationTimedOutException;
import org.messaginghub.amqperative.exceptions.ClientResourceClosedException;
import org.messaginghub.amqperative.exceptions.ClientSendTimedOutException;
import org.messaginghub.amqperative.exceptions.ClientUnsupportedOperationException;
import org.messaginghub.amqperative.futures.AsyncResult;
import org.messaginghub.amqperative.futures.ClientFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proton based AMQP Sender
 */
public class ClientSender implements Sender {

    private static final Logger LOG = LoggerFactory.getLogger(ClientSender.class);

    private static final AtomicIntegerFieldUpdater<ClientSender> CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientSender.class, "closed");

    private final ClientFuture<Sender> openFuture;
    private final ClientFuture<Sender> closeFuture;

    private volatile int closed;
    private ClientException failureCause;
    private boolean remoteRejectedOpen;

    private final Deque<InFlightSend> blocked = new ArrayDeque<InFlightSend>();
    private final SenderOptions options;
    private final ClientSession session;
    private final org.apache.qpid.proton4j.engine.Sender protonSender;
    private final ScheduledExecutorService executor;
    private final String senderId;
    private Consumer<ClientSender> senderRemotelyClosedHandler;

    private volatile Source remoteSource;
    private volatile Target remoteTarget;

    public ClientSender(ClientSession session, SenderOptions options, String senderId, org.apache.qpid.proton4j.engine.Sender protonSender) {
        this.options = new SenderOptions(options);
        this.session = session;
        this.senderId = senderId;
        this.executor = session.getScheduler();
        this.openFuture = session.getFutureFactory().createFuture();
        this.closeFuture = session.getFutureFactory().createFuture();
        this.protonSender = protonSender;

        // Use a tag generator that will reuse old tags.  Later we might make this configurable.
        if (protonSender.getSenderSettleMode() == SenderSettleMode.SETTLED) {
            protonSender.setDeliveryTagGenerator(ProtonDeliveryTagGenerator.BUILTIN.EMPTY.createGenerator());
        } else {
            protonSender.setDeliveryTagGenerator(ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator());
        }

        // Ensure that the sender can provide a link back to this object.
        protonSender.setLinkedResource(this);
    }

    @Override
    public String address() throws ClientException {
        final org.apache.qpid.proton4j.types.messaging.Target target;
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
    public ClientInstance client() {
        return session.client();
    }

    @Override
    public ClientSession session() {
        return session;
    }

    Sender remotelyClosedHandler(Consumer<ClientSender> handler) {
        this.senderRemotelyClosedHandler = handler;
        return this;
    }

    @Override
    public ClientFuture<Sender> openFuture() {
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
        checkClosed();
        ClientFuture<Tracker> operation = session.getFutureFactory().createFuture();
        final ProtonBuffer buffer = ClientMessageSupport.encodeMessage((ClientMessage<?>) message);

        executor.execute(() -> {
            if (protonSender.isSendable()) {
                assumeSendableAndSend(buffer, operation);
            } else {
                final InFlightSend send = new InFlightSend(buffer, operation);
                if (options.sendTimeout() > 0) {
                    send.timeout = session.scheduleRequestTimeout(
                        operation, options.sendTimeout(), () -> send.createSendTimedOutException());
                }

                blocked.offer(send);
            }
        });

        return session.request(operation, options.requestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Tracker trySend(Message<?> message) throws ClientException {
        checkClosed();
        ClientFuture<Tracker> operation = session.getFutureFactory().createFuture();
        ProtonBuffer buffer = ClientMessageSupport.encodeMessage((ClientMessage<?>) message);

        executor.execute(() -> {
            if (protonSender.isSendable()) {
                assumeSendableAndSend(buffer, operation);
            } else {
                operation.complete(null);
            }
        });

        return session.request(operation, options.requestTimeout(), TimeUnit.MILLISECONDS);
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

    //----- Internal API

    void disposition(OutgoingDelivery delivery, DeliveryState state, boolean settled) {
        checkClosed();
        executor.execute(() -> {
            delivery.disposition(state, settled);
        });
    }

    ClientSender open() {
        protonSender.localOpenHandler(sender -> handleLocalOpen(sender))
                    .localCloseHandler(sender -> handleLocalCloseOrDetach(sender))
                    .localDetachHandler(sender -> handleLocalCloseOrDetach(sender))
                    .openHandler(sender -> handleRemoteOpen(sender))
                    .closeHandler(sender -> handleRemoteCloseOrDetach(sender))
                    .detachHandler(sender -> handleRemoteCloseOrDetach(sender))
                    .deliveryStateUpdatedHandler(delivery -> handleDeliveryUpdated(delivery))
                    .creditStateUpdateHandler(linkState -> handleCreditStateUpdated(linkState))
                    .engineShutdownHandler(engine -> immediateLinkShutdown())
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
        return senderId;
    }

    boolean isClosed() {
        return closed > 0;
    }

    boolean isAnonymous() {
        return protonSender.<org.apache.qpid.proton4j.types.messaging.Target>getTarget().getAddress() == null;
    }

    boolean isDynamic() {
        return protonSender.getTarget() != null && protonSender.<org.apache.qpid.proton4j.types.messaging.Target>getTarget().isDynamic();
    }

    //----- Handlers for proton receiver events

    private void handleLocalOpen(org.apache.qpid.proton4j.engine.Sender sender) {
        if (options.openTimeout() > 0) {
            executor.schedule(() -> {
                if (!openFuture.isDone()) {
                    if (failureCause == null) {
                        failureCause = new ClientOperationTimedOutException("Sender open timed out waiting for remote to respond");
                    }

                    if (protonSender.isLocallyClosed()) {
                        // We didn't hear back from open and link was since closed so just fail
                        // the close as we don't want to doubly wait for something that can't come.
                        immediateLinkShutdown();
                    } else {
                        try {
                            sender.close();
                        } catch (Throwable error) {
                            // Connection is responding to all engine failed errors
                        }
                    }
                }
            }, options.openTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    private void handleLocalCloseOrDetach(org.apache.qpid.proton4j.engine.Sender sender) {
        if (failureCause == null) {
            failureCause = session.getFailureCause();
        }

        // If not yet remotely closed we only wait for a remote close if the session isn't
        // already failed and we have successfully opened the sender without a timeout.
        if (!session.isClosed() && failureCause == null && sender.isRemotelyOpen()) {
            final long timeout = options.closeTimeout();

            if (timeout > 0) {
                session.scheduleRequestTimeout(closeFuture, timeout, () ->
                    new ClientOperationTimedOutException("Sender close timed out waiting for remote to respond"));
            }
        } else {
            immediateLinkShutdown();
        }
    }

    private void handleRemoteOpen(org.apache.qpid.proton4j.engine.Sender sender) {
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
            remoteRejectedOpen = true;
        }
    }

    private void handleRemoteCloseOrDetach(org.apache.qpid.proton4j.engine.Sender sender) {
        if (sender.isLocallyOpen()) {
            final ClientException error;

            if (sender.getRemoteCondition() != null) {
                error = ClientErrorSupport.convertToNonFatalException(sender.getRemoteCondition());
            } else {
                error = new ClientResourceClosedException("Sender remotely closed without explanation");
            }

            if (failureCause == null) {
                failureCause = error;
            }

            try {
                senderRemotelyClosedHandler.accept(this);
            } catch (Throwable ignore) {}

            try {
                if (sender.isRemotelyDetached()) {
                    sender.detach();
                } else {
                    sender.close();
                }
            } catch (Throwable ignore) {
                LOG.trace("Error ignored from call to close sender after remote close.", ignore);
            }
        } else {
            immediateLinkShutdown();
        }
    }

    private void handleCreditStateUpdated(org.apache.qpid.proton4j.engine.Sender sender) {
        if (!blocked.isEmpty()) {
            while (sender.isSendable() && !blocked.isEmpty()) {
                LOG.trace("Dispatching previously held send");
                final InFlightSend held = blocked.poll();
                assumeSendableAndSend(held.message, held);
            }
        }

        if (sender.isDraining() && blocked.isEmpty()) {
            sender.drained();
        }
    }

    private void handleDeliveryUpdated(OutgoingDelivery delivery) {
        try {
            delivery.getLinkedResource(ClientTracker.class).processDeliveryUpdated(delivery);
        } catch (ClassCastException ccex) {
            LOG.debug("Sender received update on Delivery not linked to a Tracker: {}", delivery);
        }

        if (options.autoSettle() && delivery.isRemotelySettled()) {
            delivery.settle();
        }
    }

    void handleAnonymousRelayNotSupported() {
        if (isAnonymous() && protonSender.getState() == LinkState.IDLE) {
            failureCause = new ClientUnsupportedOperationException("Anonymous relay support not available from this connection");
            immediateLinkShutdown();
        }
    }

    //----- Send Result Tracker used for send blocked on credit

    private class InFlightSend implements AsyncResult<Tracker> {

        private final ProtonBuffer message;
        private final ClientFuture<Tracker> operation;

        private ScheduledFuture<?> timeout;

        public InFlightSend(ProtonBuffer message, ClientFuture<Tracker> operation) {
            this.message = message;
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
        public void complete(Tracker result) {
            if (timeout != null) {
                timeout.cancel(true);
            }
            operation.complete(result);
        }

        @Override
        public boolean isComplete() {
            return operation.isDone();
        }

        public ClientException createSendTimedOutException() {
            return new ClientSendTimedOutException("Timed out waiting for credit to send");
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

    private void assumeSendableAndSend(ProtonBuffer buffer, AsyncResult<Tracker> request) {
        final OutgoingDelivery delivery = protonSender.next();
        final ClientTracker tracker = new ClientTracker(this, delivery);

        delivery.setLinkedResource(tracker);

        if (protonSender.getSenderSettleMode() == SenderSettleMode.SETTLED) {
            delivery.disposition(session.getTransactionContext().enlistSendInCurrentTransaction(this), true);

            // Remote will not update this delivery so mark as acknowledged now.
            tracker.acknowledgeFuture().complete(tracker);
        } else {
            delivery.disposition(session.getTransactionContext().enlistSendInCurrentTransaction(this), false);
        }

        // TODO: How do we provide a send mode equivalent to Qpid JMS asynchronous send on NON_PERSISTENT Message.
        //       Do we complete before write for presettled senders?
        request.complete(tracker);

        delivery.writeBytes(buffer);
        // request.complete(tracker);
    }

    private void checkClosed() throws IllegalStateException {
        if (isClosed()) {
            IllegalStateException error = null;

            if (getFailureCause() == null) {
                error = new IllegalStateException("The Sender is closed");
            } else {
                error = new IllegalStateException("The Sender was closed due to an unrecoverable error.");
                error.initCause(getFailureCause());
            }

            throw error;
        }
    }

    private void immediateLinkShutdown() {
        CLOSED_UPDATER.lazySet(this, 1);
        if (failureCause == null) {
            if (session.getFailureCause() != null) {
                failureCause = session.getFailureCause();
            } else if (session.getEngine().failureCause() != null) {
                failureCause = ClientExceptionSupport.createOrPassthroughFatal(session.getEngine().failureCause());
            }
        }

        try {
            if (protonSender.isRemotelyDetached()) {
                protonSender.detach();
            } else {
                protonSender.close();
            }
        } catch (Throwable ignore) {
        }

        if (failureCause != null) {
            openFuture.failed(failureCause);
            // Session is in failed state so an error from sender close won't help user or the
            // remote closed the sender with an error by omitting the inbound source
            if (remoteRejectedOpen || session.getFailureCause() != null) {
                closeFuture.complete(this);
            } else {
                closeFuture.failed(failureCause);
            }
        } else {
            openFuture.complete(this);
            closeFuture.complete(this);
        }
    }
}
