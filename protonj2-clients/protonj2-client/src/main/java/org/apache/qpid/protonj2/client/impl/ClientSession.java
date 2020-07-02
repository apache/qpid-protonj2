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
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.SenderOptions;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.SessionOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceClosedException;
import org.apache.qpid.protonj2.client.futures.AsyncResult;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.client.futures.ClientFutureFactory;
import org.apache.qpid.protonj2.client.util.NoOpExecutor;
import org.apache.qpid.protonj2.engine.Engine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client implementation of the Session API.
 */
public class ClientSession implements Session {

    private static final Logger LOG = LoggerFactory.getLogger(ClientSession.class);

    private static final AtomicIntegerFieldUpdater<ClientSession> CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientSession.class, "closed");

    private volatile int closed;
    private volatile ClientException failureCause;

    private final ClientFuture<Session> openFuture;
    private final ClientFuture<Session> closeFuture;

    private final SessionOptions options;
    private final ClientConnection connection;
    private final org.apache.qpid.protonj2.engine.Session protonSession;
    private final ScheduledExecutorService serializer;
    private final String sessionId;
    private final ClientSenderBuilder senderBuilder;
    private final ClientReceiverBuilder receiverBuilder;
    private final ClientTransactionContext txnContext;

    private volatile ThreadPoolExecutor deliveryExecutor;
    private final AtomicReference<Thread> deliveryThread = new AtomicReference<Thread>();

    public ClientSession(SessionOptions options, ClientConnection connection, org.apache.qpid.protonj2.engine.Session session) {
        this.options = new SessionOptions(options);
        this.connection = connection;
        this.protonSession = session;
        this.sessionId = connection.nextSessionId();
        this.serializer = connection.getScheduler();
        this.openFuture = connection.getFutureFactory().createFuture();
        this.closeFuture = connection.getFutureFactory().createFuture();
        this.senderBuilder = new ClientSenderBuilder(this);
        this.receiverBuilder = new ClientReceiverBuilder(this);
        this.txnContext = new ClientTransactionContext(this);

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
    public Future<Session> close() {
        return doClose(null);
    }

    @Override
    public Future<Session> close(ErrorCondition error) {
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
        checkClosed();
        Objects.requireNonNull(address, "Cannot create a receiver with a null address");
        final ClientFuture<Receiver> createReceiver = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosed();
                createReceiver.complete(internalOpenReceiver(address, receiverOptions));
            } catch (Throwable error) {
                createReceiver.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, createReceiver, options.requestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Receiver openDurableReceiver(String address, String subscriptionName) throws ClientException {
        return openDurableReceiver(address, subscriptionName, null);
    }

    @Override
    public Receiver openDurableReceiver(String address, String subscriptionName, ReceiverOptions receiverOptions) throws ClientException {
        checkClosed();
        Objects.requireNonNull(address, "Cannot create a receiver with a null address");
        final ClientFuture<Receiver> createReceiver = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosed();
                createReceiver.complete(internalOpenDurableReceiver(address, subscriptionName, receiverOptions));
            } catch (Throwable error) {
                createReceiver.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, createReceiver, options.requestTimeout(), TimeUnit.MILLISECONDS);
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
        checkClosed();
        final ClientFuture<Receiver> createReceiver = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosed();
                createReceiver.complete(internalOpenDynamicReceiver(dynamicNodeProperties, receiverOptions));
            } catch (Throwable error) {
                createReceiver.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, createReceiver, options.requestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Sender openSender(String address) throws ClientException {
        return openSender(address, null);
    }

    @Override
    public Sender openSender(String address, SenderOptions senderOptions) throws ClientException {
        checkClosed();
        Objects.requireNonNull(address, "Cannot create a sender with a null address");
        final ClientFuture<Sender> createSender = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosed();
                createSender.complete(internalOpenSender(address, senderOptions));
            } catch (Throwable error) {
                createSender.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, createSender, options.requestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Sender openAnonymousSender() throws ClientException {
        return openAnonymousSender(null);
    }

    @Override
    public Sender openAnonymousSender(SenderOptions senderOptions) throws ClientException {
        checkClosed();
        final ClientFuture<Sender> createSender = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosed();
                createSender.complete(internalOpenAnonymousSender(senderOptions));
            } catch (Throwable error) {
                createSender.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, createSender, options.requestTimeout(), TimeUnit.MILLISECONDS);
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
        checkClosed();
        final ClientFuture<Session> beginFuture = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosed();
                txnContext.begin(beginFuture);
            } catch (Throwable error) {
                beginFuture.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, beginFuture, options.requestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Session commitTransaction() throws ClientException {
        checkClosed();
        final ClientFuture<Session> commitFuture = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosed();
                txnContext.commit(commitFuture, false);
            } catch (Throwable error) {
                commitFuture.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, commitFuture, options.requestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Session rollbackTransaction() throws ClientException {
        checkClosed();
        final ClientFuture<Session> rollbackFuture = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosed();
                txnContext.rollback(rollbackFuture, false);
            } catch (Throwable error) {
                rollbackFuture.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(this, rollbackFuture, options.requestTimeout(), TimeUnit.MILLISECONDS);
    }

    //----- Internal resource open APIs expected to be called from the connection event loop

    ClientReceiver internalOpenReceiver(String address, ReceiverOptions receiverOptions) throws ClientException {
        return receiverBuilder.receiver(address, receiverOptions).open();
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

    //----- Internal API accessible for use within the package

    ClientSession open() {
        protonSession.localOpenHandler(session -> handleLocalOpen(session))
                     .localCloseHandler(session -> handleLocalClose(session))
                     .openHandler(session -> handleRemoteOpen(session))
                     .closeHandler(session -> handleRemoteClose(session))
                     .engineShutdownHandler(engine -> immediateSessionShutdown());

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

    Engine getEngine() {
        return connection.getEngine();
    }

    ClientFutureFactory getFutureFactory() {
        return connection.getFutureFactory();
    }

    Executor getDeliveryExecutor() {
        ThreadPoolExecutor exec = deliveryExecutor;
        if (exec == null) {
            synchronized (options) {
                if (deliveryExecutor == null) {
                    if (!isClosed()) {
                        deliveryExecutor = exec = createExecutor("delivery dispatcher", deliveryThread);
                    } else {
                        return NoOpExecutor.INSTANCE;
                    }
                } else {
                    exec = deliveryExecutor;
                }
            }
        }

        return exec;
    }

    ClientException getFailureCause() {
        return failureCause;
    }

    boolean isClosed() {
        return closed > 0;
    }

    ScheduledFuture<?> scheduleRequestTimeout(final AsyncResult<?> request, long timeout, final ClientException error) {
        return connection.scheduleRequestTimeout(request, timeout, error);
    }

    ScheduledFuture<?> scheduleRequestTimeout(final AsyncResult<?> request, long timeout, Supplier<ClientException> errorSupplier) {
        return connection.scheduleRequestTimeout(request, timeout, errorSupplier);
    }

    <T> T request(Object requestor, ClientFuture<T> request, long timeout, TimeUnit units) throws ClientException {
        return connection.request(requestor, request, timeout, units);
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

    private void checkClosed() throws ClientIllegalStateException {
        if (isClosed()) {
            ClientIllegalStateException error = null;

            if (failureCause == null) {
                error = new ClientIllegalStateException("The Session is closed");
            } else {
                error = new ClientIllegalStateException("The Session was closed due to an unrecoverable error.");
                error.initCause(failureCause);
            }

            throw error;
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

    private ThreadPoolExecutor createExecutor(final String threadNameSuffix, AtomicReference<Thread> threadTracker) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(),
            new ClientThreadFactory("ClientSession ["+ sessionId + "] " + threadNameSuffix, true, threadTracker));

        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardOldestPolicy() {

            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                // Completely ignore the task if the session has closed.
                if (!isClosed()) {
                    LOG.trace("Task {} rejected from executor: {}", r, e);
                    super.rejectedExecution(r, e);
                }
            }
        });

        return executor;
    }

    //----- Handle Events from the Proton Session

    private void handleLocalOpen(org.apache.qpid.protonj2.engine.Session session) {
        if (options.openTimeout() > 0) {
            serializer.schedule(() -> {
                if (!openFuture.isDone()) {
                    if (failureCause == null) {
                        failureCause = new ClientOperationTimedOutException("Session open timed out waiting for remote to respond");
                    }

                    if (protonSession.isLocallyClosed()) {
                        // We didn't hear back from open and session was since closed so just fail
                        // the close as we don't want to doubly wait for something that can't come.
                        immediateSessionShutdown();
                    } else {
                        try {
                            protonSession.close();
                        } catch (Throwable error) {
                            // Connection is responding to all engine failed errors
                        }
                    }
                }
            }, options.openTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    private void handleLocalClose(org.apache.qpid.protonj2.engine.Session session) {
        if (failureCause == null) {
            failureCause = connection.getFailureCause();
        }

        // If not yet remotely closed we only wait for a remote close if the connection isn't
        // already failed and we have successfully opened the session without a timeout.
        if (!connection.isClosed() && failureCause == null && !session.isRemotelyClosed()) {
            final long timeout = options.closeTimeout();

            if (timeout > 0) {
                connection.scheduleRequestTimeout(closeFuture, timeout, () ->
                    new ClientOperationTimedOutException("Session close timed out waiting for remote to respond"));
            }
        } else {
            immediateSessionShutdown();
        }
    }

    private void handleRemoteOpen(org.apache.qpid.protonj2.engine.Session session) {
        openFuture.complete(this);
        LOG.trace("Session:{} opened successfully.", id());

        session.senders().forEach(sender -> {
            if (!sender.isLocallyOpen()) {
                try {
                    ClientSender clientSender = sender.getLinkedResource(ClientSender.class);
                    if (connection.getCapabilities().anonymousRelaySupported()) {
                        clientSender.open();
                    } else {
                        clientSender.handleAnonymousRelayNotSupported();
                    }
                } catch (ClassCastException ignore) {
                    LOG.debug("Found Sender without linked client resource on session open: {}", sender);
                }
            }
        });
    }

    private void handleRemoteClose(org.apache.qpid.protonj2.engine.Session session) {
        if (session.isLocallyOpen()) {
            final ClientException error;

            if (session.getRemoteCondition() != null) {
                error = ClientErrorSupport.convertToNonFatalException(session.getRemoteCondition());
            } else {
                error = new ClientResourceClosedException("Session remotely closed without explanation");
            }

            if (failureCause == null) {
                failureCause = error;
            }

            try {
                session.close();
            } catch (Throwable ignore) {
                LOG.trace("Error ignored from call to close session after remote close.", ignore);
            }
        } else {
            immediateSessionShutdown();
        }
    }

    private void immediateSessionShutdown() {
        CLOSED_UPDATER.lazySet(this, 1);
        if (failureCause == null) {
            if (connection.getFailureCause() != null) {
                failureCause = connection.getFailureCause();
            } else if (getEngine().failureCause() != null) {
                failureCause = ClientExceptionSupport.createOrPassthroughFatal(getEngine().failureCause());
            }
        }

        try {
            protonSession.close();
        } catch (Throwable ignore) {
        }

        if (failureCause != null) {
            openFuture.failed(failureCause);
            // Connection failed so throw from session close won't give any tangible
            if (connection.getFailureCause() != null) {
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
