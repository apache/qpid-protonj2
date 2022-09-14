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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.Link;
import org.apache.qpid.protonj2.client.LinkOptions;
import org.apache.qpid.protonj2.client.Source;
import org.apache.qpid.protonj2.client.Target;
import org.apache.qpid.protonj2.client.exceptions.ClientConnectionRemotelyClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceRemotelyClosedException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.Engine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base type used by client resources that represent an AMQP link type.
 *
 * @param <LinkType> The actual link type implementation ClientSender or ClientReceiver
 * @param <ProtonType> The proton concrete link type implementation Sender or Receiver
 */
public abstract class ClientLinkType<LinkType extends Link<LinkType>,
                                     ProtonType extends org.apache.qpid.protonj2.engine.Link<ProtonType>> implements Link<LinkType> {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @SuppressWarnings("rawtypes")
    protected static final AtomicIntegerFieldUpdater<ClientLinkType> CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientLinkType.class, "closed");

    protected final ClientFuture<LinkType> openFuture;
    protected final ClientFuture<LinkType> closeFuture;

    protected volatile int closed;
    protected ClientException failureCause;

    protected final ClientSession session;
    protected final ScheduledExecutorService executor;
    protected final String linkId;
    protected final LinkOptions<?> options;

    protected volatile Source remoteSource;
    protected volatile Target remoteTarget;

    protected Consumer<LinkType> linkRemotelyClosedHandler;

    ClientLinkType(ClientSession session, String linkId, LinkOptions<?> options) {
        this.session = session;
        this.linkId = linkId;
        this.options = options;
        this.executor = session.getScheduler();
        this.openFuture = session.getFutureFactory().createFuture();
        this.closeFuture = session.getFutureFactory().createFuture();
    }

    protected abstract LinkType self();

    protected abstract ProtonType protonLink();

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
    public ClientFuture<LinkType> closeAsync() {
        return doCloseOrDetach(true, null);
    }

    @Override
    public ClientFuture<LinkType> closeAsync(ErrorCondition error) {
        Objects.requireNonNull(error, "Error Condition cannot be null");

        return doCloseOrDetach(true, error);
    }

    @Override
    public ClientFuture<LinkType> detachAsync() {
        return doCloseOrDetach(false, null);
    }

    @Override
    public ClientFuture<LinkType> detachAsync(ErrorCondition error) {
        Objects.requireNonNull(error, "Error Condition cannot be null");

        return doCloseOrDetach(false, error);
    }

    private ClientFuture<LinkType> doCloseOrDetach(boolean close, ErrorCondition error) {
        if (CLOSED_UPDATER.compareAndSet(this, 0, 1)) {
            // Already closed by failure or shutdown so no need to queue task
            if (!closeFuture.isDone()) {
                executor.execute(() -> {
                    if (protonLink().isLocallyOpen()) {
                        try {
                            protonLink().setCondition(ClientErrorCondition.asProtonErrorCondition(error));

                            if (close) {
                                protonLink().close();
                            } else {
                                protonLink().detach();
                            }
                        } catch (Throwable ignore) {
                            closeFuture.complete(self());
                        }
                    }
                });
            }
        }
        return closeFuture;
    }

    @Override
    public String address() throws ClientException {
        if (protonLink().isSender()) {
            final org.apache.qpid.protonj2.types.messaging.Target target;
            if (isDynamic()) {
                waitForOpenToComplete();
                target = protonLink().getRemoteTarget();
            } else {
                target = protonLink().getTarget();
            }

            if (target != null) {
                return target.getAddress();
            } else {
                return null;
            }
        } else {
            if (isDynamic()) {
                waitForOpenToComplete();
                return protonLink().getRemoteSource().getAddress();
            } else {
                return protonLink().getSource() != null ? protonLink().getSource().getAddress() : null;
            }
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
        return ClientConversionSupport.toStringKeyedMap(protonLink().getRemoteProperties());
    }

    @Override
    public String[] offeredCapabilities() throws ClientException {
        waitForOpenToComplete();
        return ClientConversionSupport.toStringArray(protonLink().getRemoteOfferedCapabilities());
    }

    @Override
    public String[] desiredCapabilities() throws ClientException {
        waitForOpenToComplete();
        return ClientConversionSupport.toStringArray(protonLink().getRemoteDesiredCapabilities());
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
    public ClientFuture<LinkType> openFuture() {
        return openFuture;
    }

    final LinkType remotelyClosedHandler(Consumer<LinkType> handler) {
        this.linkRemotelyClosedHandler = handler;
        return self();
    }

    final String getId() {
        return linkId;
    }

    final void setFailureCause(ClientException failureCause) {
        this.failureCause = failureCause;
    }

    final ClientException getFailureCause() {
        if (failureCause == null) {
            return session.getFailureCause();
        } else {
            return failureCause;
        }
    }

    final boolean isClosed() {
        return closed > 0;
    }

    final boolean isDynamic() {
        if (protonLink().isSender()) {
            return protonLink().getTarget() != null && protonLink().<org.apache.qpid.protonj2.types.messaging.Target>getTarget().isDynamic();
        } else {
            return protonLink().getSource() != null && protonLink().getSource().isDynamic();
        }
    }

    final LinkType open() {
        protonLink().localOpenHandler(this::handleLocalOpen)
                    .localCloseHandler(this::handleLocalCloseOrDetach)
                    .localDetachHandler(this::handleLocalCloseOrDetach)
                    .openHandler(this::handleRemoteOpen)
                    .closeHandler(this::handleRemoteCloseOrDetach)
                    .detachHandler(this::handleRemoteCloseOrDetach)
                    .parentEndpointClosedHandler(this::handleParentEndpointClosed)
                    .engineShutdownHandler(this::handleEngineShutdown);

        protonLink().open();

        return self();
    }

    //----- Generic link state change handlers

    protected final void handleLocalOpen(ProtonType link) {
        linkSpecificLocalOpenHandler();

        if (options.openTimeout() > 0) {
            executor.schedule(() -> {
                if (!openFuture.isDone()) {
                    immediateLinkShutdown(new ClientOperationTimedOutException("Link open timed out waiting for remote to respond"));
                }
            }, options.openTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    protected final void handleLocalCloseOrDetach(ProtonType link) {
        linkSpecificLocalCloseHandler();

        // If not yet remotely closed we only wait for a remote close if the engine isn't
        // already failed and we have successfully opened the sender without a timeout.
        if (!link.getEngine().isShutdown() && failureCause == null && link.isRemotelyOpen()) {
            final long timeout = options.closeTimeout();

            if (timeout > 0) {
                session.scheduleRequestTimeout(closeFuture, timeout, () ->
                    new ClientOperationTimedOutException("Link close timed out waiting for remote to respond"));
            }
        } else {
            immediateLinkShutdown(failureCause);
        }
    }

    protected final void handleRemoteOpen(ProtonType link) {
        // Check for deferred close pending and hold completion if so
        if ((link.isSender() && link.getRemoteTarget() != null) ||
            (link.isReceiver() && link.getRemoteSource() != null)) {

            if (link.getRemoteSource() != null) {
                remoteSource = new ClientRemoteSource(link.getRemoteSource());
            }

            if (link.getRemoteTarget() != null) {
                remoteTarget = new ClientRemoteTarget(link.getRemoteTarget());
            }

            linkSpecificRemoteOpenHandler();

            openFuture.complete(self());
            LOG.trace("Link opened successfully: {}", link);
        } else {
            LOG.debug("Link opened but remote signaled close is pending: {}", link);
        }
    }

    protected final void handleRemoteCloseOrDetach(ProtonType link) {
        linkSpecificRemoteCloseHandler();

        if (link.isLocallyOpen()) {
            if (linkRemotelyClosedHandler != null) {
                try {
                    linkRemotelyClosedHandler.accept(self());
                } catch (Throwable ignore) {}
            }

            immediateLinkShutdown(ClientExceptionSupport.convertToLinkClosedException(
                link.getRemoteCondition(), "Link remotely closed without explanation from the remote"));
        } else {
            immediateLinkShutdown(failureCause);
        }
    }

    protected final void handleParentEndpointClosed(ProtonType link) {
        // Don't react if engine was shutdown and parent closed as a result instead wait to get the
        // shutdown notification and respond to that change.
        if (link.getEngine().isRunning()) {
            final ClientException failureCause;

            if (link.getConnection().getRemoteCondition() != null) {
                failureCause = ClientExceptionSupport.convertToConnectionClosedException(link.getConnection().getRemoteCondition());
            } else if (link.getSession().getRemoteCondition() != null) {
                failureCause = ClientExceptionSupport.convertToSessionClosedException(link.getSession().getRemoteCondition());
            } else if (link.getEngine().failureCause() != null) {
                failureCause = ClientExceptionSupport.convertToConnectionClosedException(link.getEngine().failureCause());
            } else if (!isClosed()) {
                failureCause = new ClientResourceRemotelyClosedException("Remote closed without a specific error condition");
            } else {
                failureCause = null;
            }

            immediateLinkShutdown(failureCause);
        }
    }

    protected final void handleEngineShutdown(Engine engine) {
        if (!isDynamic() && !session.getConnection().getEngine().isShutdown()) {
            recreateLinkForReconnect();
            open();
        } else {
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
    }

    protected final void immediateLinkShutdown(ClientException failureCause) {
        if (this.failureCause == null) {
            this.failureCause = failureCause;
        }

        try {
            if (protonLink().isRemotelyDetached()) {
                protonLink().detach();
            } else {
                protonLink().close();
            }
        } catch (Throwable ignore) {
            // Ignore
        }

        try {
            linkSpecificCleanupHandler(this.failureCause);
        } catch (Exception ex) {
            // Ignore for now, possibly log if it becomes needed
        } finally {
            if (failureCause != null) {
                openFuture.failed(failureCause);
            } else {
                openFuture.complete(self());
            }

            closeFuture.complete(self());
        }
    }

    //----- Abstract API for implementations to override

    protected abstract void linkSpecificLocalOpenHandler();

    protected abstract void linkSpecificLocalCloseHandler();

    protected abstract void linkSpecificRemoteOpenHandler();

    protected abstract void linkSpecificRemoteCloseHandler();

    protected abstract void linkSpecificCleanupHandler(ClientException failureCause);

    protected abstract void recreateLinkForReconnect();

    //----- Internal API for link implementations

    protected boolean notClosedOrFailed(ClientFuture<?> request) {
        return notClosedOrFailed(request, protonLink());
    }

    protected boolean notClosedOrFailed(ClientFuture<?> request, ProtonType protonLink) {
        if (isClosed()) {
            request.failed(new ClientIllegalStateException(
                String.format("The %s was explicitly closed", protonLink().isReceiver() ? "Receiver" : "Sender"), failureCause));
            return false;
        } else if (failureCause != null) {
            request.failed(failureCause);
            return false;
        } else if (protonLink.isLocallyClosedOrDetached()) {
            if (protonLink.getConnection().getRemoteCondition() != null) {
                request.failed(ClientExceptionSupport.convertToConnectionClosedException(protonLink.getConnection().getRemoteCondition()));
            } else if (protonLink.getSession().getRemoteCondition() != null) {
                request.failed(ClientExceptionSupport.convertToSessionClosedException(protonLink.getSession().getRemoteCondition()));
            } else if (protonLink.getEngine().failureCause() != null) {
                request.failed(ClientExceptionSupport.convertToConnectionClosedException(protonLink.getEngine().failureCause()));
            } else {
                request.failed(new ClientIllegalStateException(
                    String.format("{} closed without a specific error condition", protonLink.isSender() ? "Sender" : "Receiver")));
            }
            return false;
        } else {
            return true;
        }
    }

    protected void checkClosedOrFailed() throws ClientException {
        if (isClosed()) {
            throw new ClientIllegalStateException(
                String.format("The %s was explicitly closed", protonLink().isReceiver() ? "Receiver" : "Sender"), failureCause);
        } else if (failureCause != null) {
            throw failureCause;
        }
    }

    protected void waitForOpenToComplete() throws ClientException {
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
}
