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
import java.util.function.Supplier;

import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.SessionOptions;
import org.apache.qpid.protonj2.client.StreamReceiverOptions;
import org.apache.qpid.protonj2.client.StreamSenderOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.futures.AsyncResult;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.client.futures.ClientFutureFactory;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.Engine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client implementation of the Session API.
 */
public final class ClientSession implements Session {

    private static final Logger LOG = LoggerFactory.getLogger(ClientSession.class);

    private static final long INFINITE = -1;

    private static final AtomicIntegerFieldUpdater<ClientSession> CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientSession.class, "closed");
    private static final ClientNoOpTransactionContext NO_OP_TXN_CONTEXT = new ClientNoOpTransactionContext();

    private final ClientFuture<Session> openFuture;
    private final ClientFuture<Session> closeFuture;

    private final SessionOptions options;
    private final ClientConnection connection;
    private final org.apache.qpid.protonj2.engine.Session protonSession;
    private final ScheduledExecutorService serializer;
    private final String sessionId;
    private final ClientSenderBuilder senderBuilder;
    private final ClientReceiverBuilder receiverBuilder;

    private volatile int closed;
    private volatile ClientException failureCause;
    private ClientTransactionContext txnContext = NO_OP_TXN_CONTEXT;

    public ClientSession(ClientConnection connection, SessionOptions options, String sessionId, org.apache.qpid.protonj2.engine.Session session) {
        this.options = new SessionOptions(options);
        this.connection = connection;
        this.protonSession = session;
        this.sessionId = sessionId;
        this.serializer = connection.getScheduler();
        this.openFuture = connection.getFutureFactory().createFuture();
        this.closeFuture = connection.getFutureFactory().createFuture();
        this.senderBuilder = new ClientSenderBuilder(this);
        this.receiverBuilder = new ClientReceiverBuilder(this);

        configureSession();
    }

    @Override
    public ClientInstance client() {
        return connection.client();
    }

    @Override
    public ClientConnection connection() {
        return connection;
    }

    @Override
    public Future<Session> openFuture() {
        return openFuture;
    }

    @Override
    public void close() {
        try {
            doClose(null).get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.interrupted();
        }
    }

    @Override
    public void close(ErrorCondition error) {
        try {
            doClose(error).get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.interrupted();
        }
    }

    @Override
    public Future<Session> closeAsync() {
        return doClose(null);
    }

    @Override
    public Future<Session> closeAsync(ErrorCondition error) {
        Objects.requireNonNull(error, "Supplied error condition cannot be null");
        return doClose(error);
    }

    private Future<Session> doClose(ErrorCondition error) {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            serializer.execute(() -> {
                if (protonSession.isLocallyOpen()) {
                    try {
                        protonSession.setCondition(ClientErrorCondition.asProtonErrorCondition(error));
                        protonSession.close();
                    } catch (Throwable ignore) {
                        // Allow engine error handler to deal with this
                    }
                }
            });
        }

        return closeFuture;
    }

    @Override
    public Receiver openReceiver(String address) throws ClientException {
        return openReceiver(address, null);
    }

    @Override
    public Receiver openReceiver(String address, ReceiverOptions receiverOptions) throws ClientException {
        checkClosedOrFailed();
        Objects.requireNonNull(address, "Cannot create a receiver with a null address");
        final ClientFuture<Receiver> createReceiver = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosedOrFailed();
                createReceiver.complete(internalOpenReceiver(address, receiverOptions));
            } catch (Throwable error) {
                createReceiver.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, createReceiver);
    }

    @Override
    public Receiver openDurableReceiver(String address, String subscriptionName) throws ClientException {
        return openDurableReceiver(address, subscriptionName, null);
    }

    @Override
    public Receiver openDurableReceiver(String address, String subscriptionName, ReceiverOptions receiverOptions) throws ClientException {
        checkClosedOrFailed();
        Objects.requireNonNull(address, "Cannot create a receiver with a null address");
        final ClientFuture<Receiver> createReceiver = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosedOrFailed();
                createReceiver.complete(internalOpenDurableReceiver(address, subscriptionName, receiverOptions));
            } catch (Throwable error) {
                createReceiver.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, createReceiver);
    }

    @Override
    public Receiver openDynamicReceiver() throws ClientException {
        return openDynamicReceiver(null, null);
    }

    @Override
    public Receiver openDynamicReceiver(Map<String, Object> dynamicNodeProperties) throws ClientException {
        return openDynamicReceiver(null, null);
    }

    @Override
    public Receiver openDynamicReceiver(ReceiverOptions receiverOptions) throws ClientException {
        return openDynamicReceiver(null, null);
    }

    @Override
    public Receiver openDynamicReceiver(Map<String, Object> dynamicNodeProperties, ReceiverOptions receiverOptions) throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<Receiver> createReceiver = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosedOrFailed();
                createReceiver.complete(internalOpenDynamicReceiver(dynamicNodeProperties, receiverOptions));
            } catch (Throwable error) {
                createReceiver.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, createReceiver);
    }

    @Override
    public Sender openSender(String address) throws ClientException {
        return openSender(address, null);
    }

    @Override
    public Sender openSender(String address, SenderOptions senderOptions) throws ClientException {
        checkClosedOrFailed();
        Objects.requireNonNull(address, "Cannot create a sender with a null address");
        final ClientFuture<Sender> createSender = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosedOrFailed();
                createSender.complete(internalOpenSender(address, senderOptions));
            } catch (Throwable error) {
                createSender.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, createSender);
    }

    @Override
    public Sender openAnonymousSender() throws ClientException {
        return openAnonymousSender(null);
    }

    @Override
    public Sender openAnonymousSender(SenderOptions senderOptions) throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<Sender> createSender = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosedOrFailed();
                createSender.complete(internalOpenAnonymousSender(senderOptions));
            } catch (Throwable error) {
                createSender.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, createSender);
    }

    @Override
    public Map<String, Object> properties() throws ClientException {
        waitForOpenToComplete();
        return ClientConversionSupport.toStringKeyedMap(protonSession.getRemoteProperties());
    }

    @Override
    public String[] offeredCapabilities() throws ClientException {
        waitForOpenToComplete();
        return ClientConversionSupport.toStringArray(protonSession.getRemoteOfferedCapabilities());
    }

    @Override
    public String[] desiredCapabilities() throws ClientException {
        waitForOpenToComplete();
        return ClientConversionSupport.toStringArray(protonSession.getRemoteDesiredCapabilities());
    }

    //----- Transaction state management

    @Override
    public Session beginTransaction() throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<Session> beginFuture = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosedOrFailed();
                if (txnContext == NO_OP_TXN_CONTEXT) {
                    txnContext = new ClientLocalTransactionContext(this);
                }
                txnContext.begin(beginFuture);
            } catch (Throwable error) {
                beginFuture.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, beginFuture);
    }

    @Override
    public Session commitTransaction() throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<Session> commitFuture = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosedOrFailed();
                txnContext.commit(commitFuture, false);
            } catch (Throwable error) {
                commitFuture.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, commitFuture);
    }

    @Override
    public Session rollbackTransaction() throws ClientException {
        checkClosedOrFailed();
        final ClientFuture<Session> rollbackFuture = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosedOrFailed();
                txnContext.rollback(rollbackFuture, false);
            } catch (Throwable error) {
                rollbackFuture.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, rollbackFuture);
    }

    //----- Internal resource open APIs expected to be called from the connection event loop

    ClientReceiver internalOpenReceiver(String address, ReceiverOptions receiverOptions) throws ClientException {
        return receiverBuilder.receiver(address, receiverOptions).open();
    }

    ClientStreamReceiver internalOpenStreamReceiver(String address, StreamReceiverOptions receiverOptions) throws ClientException {
        return receiverBuilder.streamReceiver(address, receiverOptions).open();
    }

    ClientReceiver internalOpenDurableReceiver(String address, String subscriptionName, ReceiverOptions receiverOptions) throws ClientException {
        return receiverBuilder.durableReceiver(address, subscriptionName, receiverOptions).open();
    }

    ClientReceiver internalOpenDynamicReceiver(Map<String, Object> dynamicNodeProperties, ReceiverOptions receiverOptions) throws ClientException {
        return receiverBuilder.dynamicReceiver(dynamicNodeProperties, receiverOptions).open();
    }

    ClientSender internalOpenSender(String address, SenderOptions senderOptions) throws ClientException {
        return senderBuilder.sender(address, senderOptions).open();
    }

    ClientSender internalOpenAnonymousSender(SenderOptions senderOptions) throws ClientException {
        // When the connection is opened we are ok to check that the anonymous relay is supported
        // and open the sender if so, otherwise we need to wait.
        if (connection.openFuture().isDone()) {
            connection.checkAnonymousRelaySupported();
            return senderBuilder.anonymousSender(senderOptions).open();
        } else {
            return senderBuilder.anonymousSender(senderOptions);
        }
    }

    ClientStreamSender internalOpenStreamSender(String address, StreamSenderOptions senderOptions) throws ClientException {
        return senderBuilder.streamSender(address, senderOptions).open();
    }

    //----- Internal API accessible for use within the package

    ClientSession open() {
        protonSession.localOpenHandler(this::handleLocalOpen)
                     .localCloseHandler(this::handleLocalClose)
                     .openHandler(this::handleRemoteOpen)
                     .closeHandler(this::handleRemoteClose)
                     .engineShutdownHandler(this::handleEngineShutdown);

        try {
            protonSession.open();
        } catch (Throwable error) {
            // Connection is responding to all engine failed errors
        }

        return this;
    }

    ScheduledExecutorService getScheduler() {
        return serializer;
    }

    ClientFutureFactory getFutureFactory() {
        return connection.getFutureFactory();
    }

    ClientException getFailureCause() {
        return failureCause;
    }

    boolean isClosed() {
        return closed > 0;
    }

    ScheduledFuture<?> scheduleRequestTimeout(final AsyncResult<?> request, long timeout, Supplier<ClientException> errorSupplier) {
        if (timeout != INFINITE) {
            return serializer.schedule(() -> request.failed(errorSupplier.get()), timeout, TimeUnit.MILLISECONDS);
        } else {
            return null;
        }
    }

    <T> T request(Object requestor, ClientFuture<T> request) throws ClientException {
        return connection.request(requestor, request);
    }

    String id() {
        return sessionId;
    }

    SessionOptions options() {
        return options;
    }

    org.apache.qpid.protonj2.engine.Session getProtonSession() {
        return protonSession;
    }

    ClientTransactionContext getTransactionContext() {
        return txnContext;
    }

    //----- Private implementation methods

    private void configureSession() {
        protonSession.setLinkedResource(this);
        protonSession.setOfferedCapabilities(ClientConversionSupport.toSymbolArray(options.offeredCapabilities()));
        protonSession.setDesiredCapabilities(ClientConversionSupport.toSymbolArray(options.desiredCapabilities()));
        protonSession.setProperties(ClientConversionSupport.toSymbolKeyedMap(options.properties()));
    }

    protected void checkClosedOrFailed() throws ClientException {
        if (isClosed()) {
            throw new ClientIllegalStateException("The Session was explicity closed", failureCause);
        } else if (failureCause != null) {
            throw failureCause;
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

    //----- Handle Events from the Proton Session

    private void handleLocalOpen(org.apache.qpid.protonj2.engine.Session session) {
        if (options.openTimeout() > 0) {
            serializer.schedule(() -> {
                if (!openFuture.isDone()) {
                    immediateSessionShutdown(new ClientOperationTimedOutException("Session open timed out waiting for remote to respond"));
                }
            }, options.openTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    private void handleLocalClose(org.apache.qpid.protonj2.engine.Session session) {
        // If not yet remotely closed we only wait for a remote close if the engine isn't
        // already failed and we have successfully opened the session without a timeout.
        if (session.isRemotelyOpen() && failureCause == null && !session.getEngine().isShutdown()) {
            final long timeout = options.closeTimeout();

            if (timeout > 0) {
                scheduleRequestTimeout(closeFuture, timeout, () ->
                    new ClientOperationTimedOutException("Session close timed out waiting for remote to respond"));
            }
        } else {
            immediateSessionShutdown(failureCause);
        }
    }

    private void handleRemoteOpen(org.apache.qpid.protonj2.engine.Session session) {
        openFuture.complete(this);
        LOG.trace("Session:{} opened successfully.", id());

        session.senders().forEach(sender -> {
            if (!sender.isLocallyOpen()) {
                ClientSender clientSender = sender.getLinkedResource();
                if (connection.getCapabilities().anonymousRelaySupported()) {
                    clientSender.open();
                } else {
                    clientSender.handleAnonymousRelayNotSupported();
                }
            }
        });
    }

    private void handleRemoteClose(org.apache.qpid.protonj2.engine.Session session) {
        if (session.isLocallyOpen()) {
            immediateSessionShutdown(ClientExceptionSupport.convertToSessionClosedException(session.getRemoteCondition()));
        } else {
            immediateSessionShutdown(failureCause);
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

        immediateSessionShutdown(failureCause);
    }

    private void immediateSessionShutdown(ClientException failureCause) {
        if (this.failureCause == null) {
            this.failureCause = failureCause;
        }

        try {
            protonSession.close();
        } catch (Exception ignore) {
        }

        if (failureCause != null) {
            openFuture.failed(failureCause);
        } else {
            openFuture.complete(this);
        }

        closeFuture.complete(this);
    }
}
