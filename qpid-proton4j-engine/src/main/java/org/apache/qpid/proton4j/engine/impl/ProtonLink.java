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
package org.apache.qpid.proton4j.engine.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.Link;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;
import org.apache.qpid.proton4j.engine.SessionState;

/**
 * Common base for Proton Senders and Receivers.
 *
 * @param <T> the type of link, {@link Sender} or {@link Receiver}.
 */
public abstract class ProtonLink<T extends Link<T>> implements Link<T> {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonLink.class);

    protected final ProtonConnection connection;
    protected final ProtonSession session;
    protected final ProtonEngine engine;

    protected final Attach localAttach = new Attach();
    protected Attach remoteAttach;

    private boolean localAttachSent;
    private boolean localDetachSent;

    private final ProtonContext context = new ProtonContext();

    private LinkState localState = LinkState.IDLE;
    private LinkState remoteState = LinkState.IDLE;

    private ErrorCondition localError;
    private ErrorCondition remoteError;

    private EventHandler<T> localOpenHandler;
    private EventHandler<T> localCloseHandler;
    private EventHandler<T> localDetachHandler;
    private EventHandler<T> sessionClosedHandler;
    private EventHandler<Engine> engineShutdownHandler;

    // Left default to null as the session or connection overrides this by default.
    private EventHandler<T> remoteOpenHandler;

    private EventHandler<T> remoteDetachHandler = (result) -> {
        LOG.trace("Link {} Remote link detach arrived at default handler.", self());
    };
    private EventHandler<T> remoteCloseHandler = (result) -> {
        LOG.trace("Link {} Remote link close arrived at default handler.", self());
    };

    /**
     * Create a new link instance with the given parent session.
     *
     * @param session
     *      The {@link Session} that this link resides within.
     * @param name
     *      The name assigned to this {@link Link}
     */
    protected ProtonLink(ProtonSession session, String name) {
        this.session = session;
        this.connection = session.getConnection();
        this.engine = session.getEngine();
        this.localAttach.setName(name);
        this.localAttach.setRole(getRole());
    }

    @Override
    public ProtonConnection getConnection() {
        return connection;
    }

    @Override
    public ProtonSession getSession() {
        return session;
    }

    @Override
    public ProtonEngine getEngine() {
        return engine;
    }

    @Override
    public String getName() {
        return localAttach.getName();
    }

    @Override
    public boolean isSender() {
        return getRole() == Role.SENDER;
    }

    @Override
    public boolean isReceiver() {
        return getRole() == Role.RECEIVER;
    }

    protected abstract T self();

    protected abstract ProtonLinkState<?> linkState();

    long getHandle() {
        return localAttach.getHandle();
    }

    @Override
    public ProtonContext getContext() {
        return context;
    }

    @Override
    public LinkState getState() {
        return localState;
    }

    @Override
    public ErrorCondition getCondition() {
        return localError;
    }

    @Override
    public T setCondition(ErrorCondition condition) {
        localError = condition == null ? null : condition.copy();

        return self();
    }

    @Override
    public LinkState getRemoteState() {
        return remoteState;
    }

    @Override
    public ErrorCondition getRemoteCondition() {
        return remoteError;
    }

    private void setRemoteCondition(ErrorCondition condition) {
        remoteError = condition == null ? null : condition.copy();
    }

    @Override
    public T open() {
        checkSessionNotClosed();
        if (getState() == LinkState.IDLE) {
            localState = LinkState.ACTIVE;
            long localHandle = session.findFreeLocalHandle(this);
            localAttach.setHandle(localHandle);
            transitionedToLocallyOpened();
            try {
                syncLocalStateWithRemote();
            } finally {
                if (localOpenHandler != null) {
                    localOpenHandler.handle(self());
                }
            }
        }

        return self();
    }

    @Override
    public T detach() {
        if (getState() == LinkState.ACTIVE) {
            engine.checkFailed("Cannot close a link while the Engine is in the failed state.");
            localState = LinkState.DETACHED;
            transitionedToLocallyDetached();
            try {
                syncLocalStateWithRemote();
            } finally {
                if (localDetachHandler != null) {
                    localDetachHandler.handle(self());
                }
            }
        }

        return self();
    }

    @Override
    public T close() {
        if (getState() == LinkState.ACTIVE) {
            engine.checkFailed("Cannot close a link while the Engine is in the failed state.");
            localState = LinkState.CLOSED;
            transitionedToLocallyClosed();
            try {
                syncLocalStateWithRemote();
            } finally {
                if (localCloseHandler != null) {
                    localCloseHandler.handle(self());
                }
            }
        }

        return self();
    }

    @Override
    public T setSenderSettleMode(SenderSettleMode senderSettleMode) {
        checkNotOpened("Cannot set Sender settlement mode on already opened Link");
        localAttach.setSenderSettleMode(senderSettleMode);
        return self();
    }

    @Override
    public SenderSettleMode getSenderSettleMode() {
        return localAttach.getSenderSettleMode();
    }

    @Override
    public T setReceiverSettleMode(ReceiverSettleMode receiverSettleMode) {
        checkNotOpened("Cannot set Receiver settlement mode already opened Link");
        localAttach.setReceiverSettleMode(receiverSettleMode);
        return self();
    }

    @Override
    public ReceiverSettleMode getReceiverSettleMode() {
        return localAttach.getReceiverSettleMode();
    }

    @Override
    public T setSource(Source source) {
        checkNotOpened("Cannot set Source on already opened Link");
        localAttach.setSource(source);
        return self();
    }

    @Override
    public Source getSource() {
        return localAttach.getSource();
    }

    @Override
    public T setTarget(Target target) {
        checkNotOpened("Cannot set Target on already opened Link");
        localAttach.setTarget(target);
        return self();
    }

    @Override
    public Target getTarget() {
        return localAttach.getTarget();
    }

    @Override
    public T setProperties(Map<Symbol, Object> properties) {
        checkNotOpened("Cannot set Properties on already opened Link");

        if (properties != null) {
            localAttach.setProperties(new LinkedHashMap<>(properties));
        } else {
            localAttach.setProperties(properties);
        }

        return self();
    }

    @Override
    public Map<Symbol, Object> getProperties() {
        if (localAttach.getProperties() != null) {
            return Collections.unmodifiableMap(localAttach.getProperties());
        }

        return null;
    }

    @Override
    public T setOfferedCapabilities(Symbol... capabilities) {
        checkNotOpened("Cannot set Offered Capabilities on already opened Link");

        if (capabilities != null) {
            localAttach.setOfferedCapabilities(Arrays.copyOf(capabilities, capabilities.length));
        } else {
            localAttach.setOfferedCapabilities(capabilities);
        }

        return self();
    }

    @Override
    public Symbol[] getOfferedCapabilities() {
        if (localAttach.getOfferedCapabilities() != null) {
            return Arrays.copyOf(localAttach.getOfferedCapabilities(), localAttach.getOfferedCapabilities().length);
        }

        return null;
    }

    @Override
    public T setDesiredCapabilities(Symbol... capabilities) {
        checkNotOpened("Cannot set Desired Capabilities on already opened Link");

        if (capabilities != null) {
            localAttach.setDesiredCapabilities(Arrays.copyOf(capabilities, capabilities.length));
        } else {
            localAttach.setDesiredCapabilities(capabilities);
        }

        return self();
    }

    @Override
    public Symbol[] getDesiredCapabilities() {
        if (localAttach.getDesiredCapabilities() != null) {
            return Arrays.copyOf(localAttach.getDesiredCapabilities(), localAttach.getDesiredCapabilities().length);
        }

        return null;
    }

    @Override
    public T setMaxMessageSize(UnsignedLong maxMessageSize) {
        checkNotOpened("Cannot set Max Message Size on already opened Link");
        localAttach.setMaxMessageSize(maxMessageSize);
        return self();
    }

    @Override
    public UnsignedLong getMaxMessageSize() {
        return localAttach.getMaxMessageSize();
    }

    @Override
    public boolean isLocallyOpen() {
        return getState() == LinkState.ACTIVE;
    }

    @Override
    public boolean isLocallyClosed() {
        return getState() == LinkState.CLOSED;
    }

    @Override
    public boolean isLocallyDetached() {
        return getState() == LinkState.DETACHED;
    }

    @Override
    public boolean isRemotelyOpen() {
        return getRemoteState() == LinkState.ACTIVE;
    }

    @Override
    public boolean isRemotelyClosed() {
        return getRemoteState() == LinkState.CLOSED;
    }

    @Override
    public boolean isRemotelyDetached() {
        return getRemoteState() == LinkState.DETACHED;
    }

    @Override
    public SenderSettleMode getRemoteSenderSettleMode() {
        if (remoteAttach != null) {
            return remoteAttach.getSenderSettleMode();
        }

        return null;
    }

    @Override
    public ReceiverSettleMode getRemoteReceiverSettleMode() {
        if (remoteAttach != null) {
            return remoteAttach.getReceiverSettleMode();
        }

        return null;
    }

    @Override
    public Source getRemoteSource() {
        if (remoteAttach != null && remoteAttach.getSource() != null) {
            return remoteAttach.getSource().copy();
        }

        return null;
    }

    @Override
    public Target getRemoteTarget() {
        if (remoteAttach != null && remoteAttach.getTarget() != null) {
            return remoteAttach.getTarget().copy();
        }

        return null;
    }

    @Override
    public Symbol[] getRemoteOfferedCapabilities() {
        if (remoteAttach != null && remoteAttach.getOfferedCapabilities() != null) {
            return Arrays.copyOf(remoteAttach.getOfferedCapabilities(), remoteAttach.getOfferedCapabilities().length);
        }

        return null;
    }

    @Override
    public Symbol[] getRemoteDesiredCapabilities() {
        if (remoteAttach != null && remoteAttach.getDesiredCapabilities() != null) {
            return Arrays.copyOf(remoteAttach.getDesiredCapabilities(), remoteAttach.getDesiredCapabilities().length);
        }

        return null;
    }

    @Override
    public Map<Symbol, Object> getRemoteProperties() {
        if (remoteAttach != null && remoteAttach.getProperties() != null) {
            return Collections.unmodifiableMap(remoteAttach.getProperties());
        }

        return null;
    }

    @Override
    public UnsignedLong getRemoteMaxMessageSize() {
        if (remoteAttach != null && remoteAttach.getMaxMessageSize() != null) {
            return remoteAttach.getMaxMessageSize();
        }

        return null;
    }

    //----- Event registration methods

    @Override
    public T localOpenHandler(EventHandler<T> localOpenHandler) {
        this.localOpenHandler = localOpenHandler;
        return self();
    }

    @Override
    public T localCloseHandler(EventHandler<T> localCloseHandler) {
        this.localCloseHandler = localCloseHandler;
        return self();
    }

    @Override
    public T localDetachHandler(EventHandler<T> localDetachHandler) {
        this.localDetachHandler = localDetachHandler;
        return self();
    }

    @Override
    public T openHandler(EventHandler<T> remoteOpenHandler) {
        this.remoteOpenHandler = remoteOpenHandler;
        return self();
    }

    @Override
    public T detachHandler(EventHandler<T> remoteDetachHandler) {
        this.remoteDetachHandler = remoteDetachHandler;
        return self();
    }

    @Override
    public T closeHandler(EventHandler<T> remoteCloseHandler) {
        this.remoteCloseHandler = remoteCloseHandler;
        return self();
    }

    @Override
    public T sessionClosedHandler(EventHandler<T> sessionClosedEventHandler) {
        this.sessionClosedHandler = sessionClosedEventHandler;
        return self();
    }

    @Override
    public T engineShutdownHandler(EventHandler<Engine> engineShutdownEventHandler) {
        this.engineShutdownHandler = engineShutdownEventHandler;
        return self();
    }

    EventHandler<Engine> engineShutdownHandler() {
        return engineShutdownHandler;
    }

    //----- Abstract Link methods needed to implement a fully functional Link type

    protected abstract void transitionedToLocallyOpened();

    protected abstract void transitionedToLocallyDetached();

    protected abstract void transitionedToLocallyClosed();

    protected abstract void transitionToRemotelyOpenedState();

    protected abstract void transitionToRemotelyDetachedState();

    protected abstract void transitionToRemotelyCosedState();

    protected abstract void transitionToParentLocallyClosedState();

    protected abstract void transitionToParentRemotelyClosedState();

    //----- Process local events from the parent session

    void handleSessionStateChanged(ProtonSession session) {
        switch (session.getState()) {
            case IDLE:
                return;
            case ACTIVE:
                syncLocalStateWithRemote();
                return;
            case CLOSED:
                processParentSessionLocallyClosed();
                return;
        }
    }

    void processParentSessionLocallyClosed() {
        transitionToParentLocallyClosedState();

        try {
            sessionClosedHandler.handle(self());
        } catch (Throwable ignored) {}
    }

    void processParentConnectionLocallyClosed() {
        transitionToParentLocallyClosedState();
    }

    private void syncLocalStateWithRemote() {
        switch (getState()) {
            case IDLE:
                return;
            case ACTIVE:
                trySendLocalAttach();
                break;
            case CLOSED:
            case DETACHED:
                trySendLocalAttach();
                trySendLocalDetach(isLocallyClosed());
                break;
            default:
                throw new IllegalStateException("Link is in unknown state and cannot proceed");
        }
    }

    void handleEngineShutdown(ProtonEngine protonEngine) {
        try {
            engineShutdownHandler.handle(protonEngine);
        } catch (Throwable ignore) {}
    }

    //----- Handle incoming performatives

    void remoteAttach(Attach attach) {
        remoteAttach = attach;
        remoteState = LinkState.ACTIVE;
        linkState().remoteAttach(attach);
        transitionToRemotelyOpenedState();

        if (remoteOpenHandler != null) {
            remoteOpenHandler.handle(self());
        } else {
            if (getRole() == Role.RECEIVER) {
                if (session.receiverOpenEventHandler() != null) {
                    session.receiverOpenEventHandler().handle((Receiver) this);
                } else if (connection.receiverOpenEventHandler() != null) {
                    connection.receiverOpenEventHandler().handle((Receiver) this);
                } else {
                    LOG.info("Receiver opened but no event handler registered to inform: {}", this);
                }
            } else {
                if (session.senderOpenEventHandler() != null) {
                    session.senderOpenEventHandler().handle((Sender) this);
                } else if (connection.senderOpenEventHandler() != null) {
                    connection.senderOpenEventHandler().handle((Sender) this);
                } else {
                    LOG.info("Sender opened but no event handler registered to inform: {}", this);
                }
            }
        }
    }

    ProtonLink<?> remoteDetach(Detach detach) {
        setRemoteCondition(detach.getError());

        linkState().remoteDetach(detach);

        if (detach.getClosed()) {
            remoteState = LinkState.CLOSED;
            transitionToRemotelyCosedState();
            if (remoteCloseHandler != null) {
                remoteCloseHandler.handle(self());
            }
        } else {
            remoteState = LinkState.DETACHED;
            transitionToRemotelyDetachedState();
            if (remoteDetachHandler != null) {
                remoteDetachHandler.handle(self());
            }
        }

        return this;
    }

    ProtonIncomingDelivery remoteTransfer(Transfer transfer, ProtonBuffer payload) {
        return linkState().remoteTransfer(transfer, payload);
    }

    ProtonLink<?> remoteFlow(Flow flow) {
        linkState().remoteFlow(flow);
        return this;
    }

    //----- Internal methods

    boolean wasLocalAttachSent() {
        return localAttachSent;
    }

    boolean wasLocalDetachSent() {
        return localDetachSent;
    }

    private void trySendLocalAttach() {
        if (!wasLocalAttachSent()) {
            if ((session.isLocallyOpen() && session.wasLocalBeginSent()) &&
                (connection.isLocallyOpen() && connection.wasLocalOpenSent())) {

                localAttachSent = true;
                session.getEngine().fireWrite(
                    linkState().configureAttach(localAttach), session.getLocalChannel(), null, null);
            }
        }
    }

    private void trySendLocalDetach(boolean closed) {
        if (!wasLocalDetachSent()) {
            if ((session.isLocallyOpen() && session.wasLocalBeginSent()) &&
                (connection.isLocallyOpen() && connection.wasLocalOpenSent()) && !engine.isShutdown()) {

                Detach detach = new Detach();
                detach.setHandle(localAttach.getHandle());
                detach.setClosed(closed);
                detach.setError(getCondition());

                session.freeLink(this);
                localDetachSent = true;
                session.getEngine().fireWrite(detach, session.getLocalChannel(), null, null);
            }
        }
    }

    protected void checkNotOpened(String errorMessage) {
        if (localState.ordinal() > LinkState.IDLE.ordinal()) {
            throw new IllegalStateException(errorMessage);
        }
    }

    protected void checkNotClosed(String errorMessage) {
        if (localState.ordinal() > LinkState.ACTIVE.ordinal()) {
            throw new IllegalStateException(errorMessage);
        }
    }

    private void checkSessionNotClosed() {
        if (session.getState() == SessionState.CLOSED || session.getRemoteState() == SessionState.CLOSED) {
            throw new IllegalStateException("Cannot open link for session that has already been closed.");
        }
    }

    abstract boolean isDeliveryCountInitialised();
}
