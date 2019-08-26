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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.engine.impl.ProtonEngine;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.ReceiverOptions;
import org.messaginghub.amqperative.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.Session;
import org.messaginghub.amqperative.SessionOptions;
import org.messaginghub.amqperative.client.exceptions.ClientExceptionSupport;
import org.messaginghub.amqperative.futures.ClientFuture;
import org.messaginghub.amqperative.futures.ClientFutureFactory;
import org.messaginghub.amqperative.util.NoOpExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client implementation of the Session API.
 */
public class ClientSession implements Session {

    private static final Logger LOG = LoggerFactory.getLogger(ClientSession.class);

    private final ClientFuture<Session> openFuture;
    private final ClientFuture<Session> closeFuture;

    private final ClientSessionOptions options;
    private final ClientConnection connection;
    private final org.apache.qpid.proton4j.engine.Session session;
    private final ScheduledExecutorService serializer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final String sessionId;
    private volatile int senderCounter;
    private volatile int receiverCounter;

    private volatile ThreadPoolExecutor deliveryExecutor;
    private final AtomicReference<Thread> deliveryThread = new AtomicReference<Thread>();
    private final AtomicReference<Throwable> failureCause = new AtomicReference<>();

    // TODO - Ensure closed resources are removed from these
    private final List<ClientSender> senders = new ArrayList<>();
    private final List<ClientReceiver> receivers = new ArrayList<>();

    public ClientSession(SessionOptions options, ClientConnection connection, org.apache.qpid.proton4j.engine.Session session) {
        this.options = new ClientSessionOptions(options);
        this.connection = connection;
        this.session = session;
        this.sessionId = connection.nextSessionId();
        this.serializer = connection.getScheduler();
        this.openFuture = connection.getFutureFactory().createFuture();
        this.closeFuture = connection.getFutureFactory().createFuture();
    }

    @Override
    public Future<Session> openFuture() {
        return openFuture;
    }

    @Override
    public Future<Session> close() {
        if (closed.compareAndSet(false, true) && !openFuture.isFailed()) {
            serializer.execute(() -> {
                session.close();
            });
        }
        return closeFuture;
    }

    @Override
    public Receiver createReceiver(String address) throws ClientException {
        checkClosed();
        ClientFuture<Receiver> createReceiver = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosed();
                createReceiver.complete(internalCreateReceiver(address, new ReceiverOptions()).open());
            } catch (Throwable error) {
                createReceiver.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(createReceiver, options.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Receiver createReceiver(String address, ReceiverOptions receiverOptions) throws ClientException {
        checkClosed();
        ClientFuture<Receiver> createReceiver = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosed();
                createReceiver.complete(internalCreateReceiver(address, receiverOptions).open());
            } catch (Throwable error) {
                createReceiver.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(createReceiver, options.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Sender createSender(String address) throws ClientException {
        return createSender(address, new ClientSenderOptions());
    }

    @Override
    public Sender createSender(String address, SenderOptions senderOptions) throws ClientException {
        checkClosed();
        ClientFuture<Sender> createSender = getFutureFactory().createFuture();

        serializer.execute(() -> {
            try {
                checkClosed();
                createSender.complete(internalCreateSender(address, senderOptions).open());
            } catch (Throwable error) {
                createSender.failed(ClientExceptionSupport.createNonFatalOrPassthrough(error));
            }
        });

        return connection.request(createSender, options.getRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    //----- Internal API accessible for use within the package

    ClientReceiver internalCreateReceiver(String address, ReceiverOptions options) {
        String name = options.getLinkName();
        if (name == null) {
            //TODO: use container-id + counter rather than UUID?
            name = "reciever-" + UUID.randomUUID();
        }

        ClientReceiver result = new ClientReceiver(options, this, session.receiver(name), address);
        receivers.add(result);
        return result;
    }

    ClientSender internalCreateSender(String address, SenderOptions options) {
        String name = options.getLinkName();
        if (name == null) {
            //TODO: use container-id + counter rather than UUID?
            name = "sender-" + UUID.randomUUID();
        }

        ClientSender result = new ClientSender(options, this, session.sender(name), address);
        senders.add(result);
        return result;
    }

    ClientSession open() {
        serializer.execute(() -> {
            session.openHandler(result -> {
                if (result.succeeded()) {
                    openFuture.complete(this);
                    LOG.trace("Connection session opened successfully");
                } else {
                    openFuture.failed(ClientExceptionSupport.createNonFatalOrPassthrough(result.error()));
                    LOG.error("Connection session failed to open: ", result.error());
                }
            });
            session.closeHandler(result -> {
                closed.set(true);
                closeFuture.complete(this);
            });

            options.configureSession(session).open();
        });

        return this;
    }

    ScheduledExecutorService getScheduler() {
        return serializer;
    }

    ProtonEngine getEngine() {
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
                    if (!closed.get()) {
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

    void setFailureCause(Throwable failureCause) {
        this.failureCause.set(failureCause);
    }

    Throwable getFailureCause() {
        return failureCause.get();
    }

    String nextReceiverId() {
        return sessionId + ":" + (++receiverCounter);
    }

    String nextSenderId() {
        return sessionId + ":" + (++senderCounter);
    }

    //----- Private implementation methods

    private void checkClosed() throws IllegalStateException {
        if (closed.get()) {
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
                if (!closed.get()) {
                    LOG.trace("Task {} rejected from executor: {}", r, e);
                    super.rejectedExecution(r, e);
                }
            }
        });

        return executor;
    }
}
