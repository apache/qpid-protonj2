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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.qpid.proton4j.engine.Engine;
import org.messaginghub.amqperative.Client;
import org.messaginghub.amqperative.Connection;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.ReceiverOptions;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.Session;
import org.messaginghub.amqperative.SessionOptions;
import org.messaginghub.amqperative.futures.AsyncResult;
import org.messaginghub.amqperative.futures.ClientFuture;
import org.messaginghub.amqperative.futures.ClientFutureFactory;
import org.messaginghub.amqperative.impl.exceptions.ClientExceptionSupport;
import org.messaginghub.amqperative.util.NoOpExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client implementation of the Session API.
 */
public class ClientSession implements Session {

    private static final Logger LOG = LoggerFactory.getLogger(ClientSession.class);

    private static final AtomicIntegerFieldUpdater<ClientSession> CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientSession.class, "closed");

    private final ClientFuture<Session> openFuture;
    private final ClientFuture<Session> closeFuture;

    private final SessionOptions options;
    private final ClientConnection connection;
    private final org.apache.qpid.proton4j.engine.Session protonSession;
    private final ScheduledExecutorService serializer;
    private final String sessionId;
    private volatile int closed;
    private final AtomicInteger senderCounter = new AtomicInteger();
    private final AtomicInteger receiverCounter = new AtomicInteger();

    private volatile ThreadPoolExecutor deliveryExecutor;
    private final AtomicReference<Thread> deliveryThread = new AtomicReference<Thread>();
    private final AtomicReference<ClientException> failureCause = new AtomicReference<>();

    // TODO - Ensure closed resources are removed from these
    private final List<ClientSender> senders = new ArrayList<>();
    private final List<ClientReceiver> receivers = new ArrayList<>();

    public ClientSession(SessionOptions options, ClientConnection connection, org.apache.qpid.proton4j.engine.Session session) {
        this.options = new SessionOptions(options);
        this.connection = connection;
        this.protonSession = session;
        this.sessionId = connection.nextSessionId();
        this.serializer = connection.getScheduler();
        this.openFuture = connection.getFutureFactory().createFuture();
        this.closeFuture = connection.getFutureFactory().createFuture();

        configureSession();
    }

    @Override
    public Client getClient() {
        return connection.getClient();
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public Future<Session> openFuture() {
        return openFuture;
    }

    @Override
    public Future<Session> close() {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1) && !openFuture.isFailed()) {
            serializer.execute(() -> {
                protonSession.close();
            });
        }
        return closeFuture;
    }

    @Override
    public Receiver openReceiver(String address) throws ClientException {
        return openReceiver(address, getDefaultReceiverOptions());
    }

    @Override
    public Receiver openReceiver(String address, ReceiverOptions receiverOptions) throws ClientException {
        checkClosed();
        Objects.requireNonNull(address, "Cannot create a receiver with a null address");
        final ClientFuture<Receiver> createReceiver = getFutureFactory().createFuture();
        final ReceiverOptions receiverOpts = receiverOptions == null ? getDefaultReceiverOptions() : receiverOptions;

        serializer.execute(() -> {
            try {
                checkClosed();
                createReceiver.complete(internalCreateReceiver(address, receiverOpts).open());
            } catch (Throwable error) {
                createReceiver.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(createReceiver, options.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Receiver openDynamicReceiver() throws ClientException {
        return openDynamicReceiver(getDefaultDynamicReceiverOptions());
    }

    @Override
    public Receiver openDynamicReceiver(ReceiverOptions receiverOptions) throws ClientException {
        checkClosed();

        if (!receiverOptions.isDynamic()) {
            throw new IllegalArgumentException("Dynamic receiver requires the options configured for dynamic.");
        }

        final ClientFuture<Receiver> createReceiver = getFutureFactory().createFuture();
        final ReceiverOptions receiverOpts = receiverOptions == null ? getDefaultReceiverOptions() : receiverOptions;

        serializer.execute(() -> {
            try {
                checkClosed();
                createReceiver.complete(internalCreateReceiver(null, receiverOpts).open());
            } catch (Throwable error) {
                createReceiver.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(createReceiver, options.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Sender openSender(String address) throws ClientException {
        return openSender(address, getDefaultSenderOptions());
    }

    @Override
    public Sender openSender(String address, SenderOptions senderOptions) throws ClientException {
        checkClosed();
        Objects.requireNonNull(address, "Cannot create a sender with a null address");
        final ClientFuture<Sender> createSender = getFutureFactory().createFuture();
        final SenderOptions senderOpts = senderOptions == null ? getDefaultSenderOptions() : senderOptions;

        serializer.execute(() -> {
            try {
                checkClosed();
                createSender.complete(internalCreateSender(address, senderOpts).open());
            } catch (Throwable error) {
                createSender.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(createSender, options.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Sender openAnonymousSender() throws ClientException {
        return openAnonymousSender(getDefaultSenderOptions());
    }

    @Override
    public Sender openAnonymousSender(SenderOptions senderOptions) throws ClientException {
        checkClosed();
        final ClientFuture<Sender> createSender = getFutureFactory().createFuture();
        final SenderOptions senderOpts = senderOptions == null ? getDefaultSenderOptions() : senderOptions;

        serializer.execute(() -> {
            try {
                checkClosed();
                createSender.complete(internalCreateSender(null, senderOpts).open());
            } catch (Throwable error) {
                createSender.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(createSender, options.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    //----- Internal API accessible for use within the package

    ClientReceiver internalCreateReceiver(String address, ReceiverOptions options) {
        ClientReceiver result = new ClientReceiver(options, this, address);
        receivers.add(result);  // TODO - We could use the protonSession to track all links ?
        return result;
    }

    ClientSender internalCreateSender(String address, SenderOptions options) {
        ClientSender result = new ClientSender(options, this, address);
        senders.add(result);  // TODO - We could use the protonSession to track all links ?
        return result;
    }

    // TODO - Refactor to allow this to move to ClientConnectionSession
    ClientConnectionSender internalCreateConnectionSender(SenderOptions options) {
        ClientConnectionSender result = new ClientConnectionSender(options, this, null);
        senders.add(result);  // TODO - We could use the protonSession to track all links ?
        return result;
    }

    ClientSession open() {
        protonSession.openHandler(result -> {
            openFuture.complete(this);
            LOG.trace("Connection session opened successfully");
        });
        protonSession.closeHandler(result -> {
            if (result.getRemoteCondition() != null) {
                // TODO - Process as failure cause if none set
            }
            CLOSED_UPDATER.lazySet(this, 1);
            closeFuture.complete(this);
        });

        protonSession.open();

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

    void setFailureCause(ClientException failureCause) {
        this.failureCause.set(failureCause);
    }

    ClientException getFailureCause() {
        return failureCause.get();
    }

    String nextReceiverId() {
        return sessionId + ":" + receiverCounter.incrementAndGet();
    }

    String nextSenderId() {
        return sessionId + ":" + senderCounter.incrementAndGet();
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

    <T> T request(ClientFuture<T> request, long timeout, TimeUnit units) throws ClientException {
        return connection.request(request, timeout, units);
    }

    org.apache.qpid.proton4j.engine.Session getProtonSession() {
        return protonSession;
    }

    //----- Private implementation methods

    private void configureSession() {
        protonSession.setOfferedCapabilities(ClientConversionSupport.toSymbolArray(options.getOfferedCapabilities()));
        protonSession.setDesiredCapabilities(ClientConversionSupport.toSymbolArray(options.getDesiredCapabilities()));
        protonSession.setProperties(ClientConversionSupport.toSymbolKeyedMap(options.getProperties()));
    }

    private void checkClosed() throws IllegalStateException {
        if (isClosed()) {
            IllegalStateException error = null;

            if (failureCause.get() == null) {
                error = new IllegalStateException("The Session is closed");
            } else {
                error = new IllegalStateException("The Session was closed due to an unrecoverable error.");
                error.initCause(failureCause.get());
            }

            throw error;
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

    // TODO - Notify links and clean up resources etc.
    void connectionClosed(ClientException error) {
        CLOSED_UPDATER.set(this, 1);

        if (error != null) {
            failureCause.compareAndSet(null, error);
            openFuture.failed(error);
        } else {
            openFuture.complete(this);
        }

        closeFuture.complete(this);
    }

    private SenderOptions defaultSenderOptions;
    private ReceiverOptions defaultReceivernOptions;
    private ReceiverOptions defaultDynamicReceivernOptions;

    /*
     * Sender options used when none specified by the caller creating a new sender.
     */
    SenderOptions getDefaultSenderOptions() {
        SenderOptions senderOptions = defaultSenderOptions;
        if (senderOptions == null) {
            synchronized (this) {
                senderOptions = defaultSenderOptions;
                if (senderOptions == null) {
                    senderOptions = new SenderOptions();
                    senderOptions.setOpenTimeout(options.getOpenTimeout());
                    senderOptions.setCloseTimeout(options.getCloseTimeout());
                    senderOptions.setRequestTimeout(options.getRequestTimeout());
                    senderOptions.setSendTimeout(options.getSendTimeout());
                }

                defaultSenderOptions = senderOptions;
            }
        }

        return senderOptions;
    }

    /*
     * Receiver options used when none specified by the caller creating a new receiver.
     */
    ReceiverOptions getDefaultReceiverOptions() {
        ReceiverOptions receiverOptions = defaultReceivernOptions;
        if (receiverOptions == null) {
            synchronized (this) {
                receiverOptions = defaultReceivernOptions;
                if (receiverOptions == null) {
                    receiverOptions = new ReceiverOptions();
                    receiverOptions.setOpenTimeout(options.getOpenTimeout());
                    receiverOptions.setCloseTimeout(options.getCloseTimeout());
                    receiverOptions.setRequestTimeout(options.getRequestTimeout());
                    receiverOptions.setSendTimeout(options.getSendTimeout());
                }

                defaultReceivernOptions = receiverOptions;
            }
        }

        return receiverOptions;
    }

    /*
     * Receiver options used when none specified by the caller creating a new dynamic receiver.
     */
    ReceiverOptions getDefaultDynamicReceiverOptions() {
        ReceiverOptions receiverOptions = defaultDynamicReceivernOptions;
        if (receiverOptions == null) {
            synchronized (this) {
                receiverOptions = defaultDynamicReceivernOptions;
                if (receiverOptions == null) {
                    receiverOptions = new ReceiverOptions();
                    receiverOptions.setOpenTimeout(options.getOpenTimeout());
                    receiverOptions.setCloseTimeout(options.getCloseTimeout());
                    receiverOptions.setRequestTimeout(options.getRequestTimeout());
                    receiverOptions.setSendTimeout(options.getSendTimeout());
                    receiverOptions.setDynamic(true);
                }

                defaultDynamicReceivernOptions = receiverOptions;
            }
        }

        return receiverOptions;
    }
}
