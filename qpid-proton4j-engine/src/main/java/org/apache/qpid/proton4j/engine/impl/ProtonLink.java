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
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.Link;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;

/**
 * Common base for Proton Senders and Receivers.
 *
 * @param <T> the type of link, {@link Sender} or {@link Receiver}.
 */
public abstract class ProtonLink<T extends Link<T>> extends ProtonEndpoint<T> implements Link<T> {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonLink.class);

    private enum LinkOperabilityState {
        OK,
        ENGINE_FAILED,
        CONNECTION_REMOTELY_CLOSED,
        CONNECTION_LOCALLY_CLOSED,
        SESSION_REMOTELY_CLOSED,
        SESSION_LOCALLY_CLOSED,
        LINK_REMOTELY_CLOSED,
        LINK_REMOTELY_DETACHED,
        LINK_LOCALLY_CLOSED,
        LINK_LOCALLY_DETACHED
    }

    protected final ProtonConnection connection;
    protected final ProtonSession session;

    protected final Attach localAttach = new Attach();
    protected Attach remoteAttach;

    private boolean localAttachSent;
    private boolean localDetachSent;

    private final ProtonLinkCreditState creditState;

    private LinkOperabilityState operability = LinkOperabilityState.OK;
    private LinkState localState = LinkState.IDLE;
    private LinkState remoteState = LinkState.IDLE;

    private EventHandler<T> localDetachHandler;
    private EventHandler<T> remoteDetachHandler;

    /**
     * Create a new link instance with the given parent session.
     *
     * @param session
     *      The {@link Session} that this link resides within.
     * @param name
     *      The name assigned to this {@link Link}
     * @param creditState
     *      The link credit state used to track credit for the link.
     */
    protected ProtonLink(ProtonSession session, String name, ProtonLinkCreditState creditState) {
        super(session.getEngine());

        this.session = session;
        this.connection = session.getConnection();
        this.creditState = creditState;
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
    public ProtonSession getParent() {
        return session;
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

    @Override
    protected abstract T self();

    long getHandle() {
        return localAttach.getHandle();
    }

    @Override
    public LinkState getState() {
        return localState;
    }

    @Override
    public LinkState getRemoteState() {
        return remoteState;
    }

    @Override
    public T open() {
        if (getState() == LinkState.IDLE) {
            checkLinkOperable("Cannot open Link");
            localState = LinkState.ACTIVE;
            long localHandle = session.findFreeLocalHandle(this);
            localAttach.setHandle(localHandle);
            transitionedToLocallyOpened();
            try {
                trySyncLocalStateWithRemote();
            } finally {
                fireLocalOpen();
            }
        }

        return self();
    }

    @Override
    public T detach() {
        if (getState() == LinkState.ACTIVE) {
            localState = LinkState.DETACHED;
            operability = LinkOperabilityState.LINK_LOCALLY_DETACHED;
            getCreditState().clearCredit();
            transitionedToLocallyDetached();
            try {
                engine.checkFailed("Closed called on already failed connection");
                trySyncLocalStateWithRemote();
            } finally {
                fireLocalDetach();
            }
        }

        return self();
    }

    @Override
    public T close() {
        if (getState() == LinkState.ACTIVE) {
            localState = LinkState.CLOSED;
            operability = LinkOperabilityState.LINK_LOCALLY_CLOSED;
            getCreditState().clearCredit();
            transitionedToLocallyClosed();
            try {
                engine.checkFailed("Detached called on already failed connection");
                trySyncLocalStateWithRemote();
            } finally {
                fireLocalClose();
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
    public T localDetachHandler(EventHandler<T> localDetachHandler) {
        this.localDetachHandler = localDetachHandler;
        return self();
    }

    EventHandler<T> localDetachHandler() {
        return localDetachHandler;
    }

    T fireLocalDetach() {
        if (localDetachHandler != null) {
            localDetachHandler.handle(self());
        } else {
            fireLocalClose();
        }

        return self();
    }

    @Override
    public T detachHandler(EventHandler<T> remoteDetachHandler) {
        this.remoteDetachHandler = remoteDetachHandler;
        return self();
    }

    EventHandler<T> detachHandler() {
        return remoteDetachHandler;
    }

    T fireRemoteDetach() {
        if (remoteDetachHandler != null) {
            remoteDetachHandler.handle(self());
        } else {
            fireRemoteClose();
        }

        return self();
    }

    //----- Link state change handlers that can be overridden by specific link implementations

    protected void transitionedToLocallyOpened() {
        // Nothing currently updated on this state change.
    }

    protected void transitionedToLocallyDetached() {
        // Nothing currently updated on this state change.
    }

    protected void transitionedToLocallyClosed() {
        // Nothing currently updated on this state change.
    }

    protected void transitionToRemotelyOpenedState() {
        // Nothing currently updated on this state change.
    }

    protected void transitionToRemotelyDetached() {
        // Nothing currently updated on this state change.
    }

    protected void transitionToRemotelyCosed() {
        // Nothing currently updated on this state change.
    }

    protected void transitionToParentLocallyClosed() {
        // Nothing currently updated on this state change.
    }

    protected void transitionToParentRemotelyClosed() {
        // Nothing currently updated on this state change.
    }

    //----- Process local events from the parent session and connection

    final void handleSessionLocallyClosed(ProtonSession session) {
        getCreditState().clearCredit();
        if (operability.ordinal() < LinkOperabilityState.SESSION_LOCALLY_CLOSED.ordinal()) {
            operability = LinkOperabilityState.SESSION_LOCALLY_CLOSED;
            transitionToParentRemotelyClosed();
        }
    }

    final void handleSessionRemotelyClosed(ProtonSession session) {
        getCreditState().clearCredit();
        if (operability.ordinal() < LinkOperabilityState.SESSION_REMOTELY_CLOSED.ordinal()) {
            operability = LinkOperabilityState.SESSION_REMOTELY_CLOSED;
            transitionToParentRemotelyClosed();
        }
    }

    final void handleConnectionLocallyClosed(ProtonConnection connection) {
        getCreditState().clearCredit();
        if (operability.ordinal() < LinkOperabilityState.CONNECTION_LOCALLY_CLOSED.ordinal()) {
            operability = LinkOperabilityState.CONNECTION_LOCALLY_CLOSED;
            transitionToParentLocallyClosed();
        }
    }

    final void handleConnectionRemotelyClosed(ProtonConnection connection) {
        getCreditState().clearCredit();
        if (operability.ordinal() < LinkOperabilityState.CONNECTION_REMOTELY_CLOSED.ordinal()) {
            operability = LinkOperabilityState.CONNECTION_REMOTELY_CLOSED;
            transitionToParentRemotelyClosed();
        }
    }

    final void handleEngineShutdown(ProtonEngine protonEngine) {
        getCreditState().clearCredit();
        if (operability.ordinal() < LinkOperabilityState.ENGINE_FAILED.ordinal()) {
            operability = LinkOperabilityState.ENGINE_FAILED;
        }

        try {
            fireEngineShutdown();
        } catch (Throwable ignore) {}
    }

    //----- Handle incoming performatives

    final void remoteAttach(Attach attach) {
        LOG.trace("Link:{} Received remote Attach:{}", self(), attach);

        remoteAttach = attach;
        remoteState = LinkState.ACTIVE;
        handleRemoteAttach(attach);
        transitionToRemotelyOpenedState();

        if (openHandler() != null) {
            fireRemoteOpen();
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

    final ProtonLink<?> remoteDetach(Detach detach) {
        LOG.trace("Link:{} Received remote Detach:{}", self(), detach);
        setRemoteCondition(detach.getError());
        getCreditState().clearCredit();

        handleRemoteDetach(detach);

        if (detach.getClosed()) {
            remoteState = LinkState.CLOSED;
            operability = LinkOperabilityState.LINK_REMOTELY_CLOSED;
            transitionToRemotelyCosed();
            fireRemoteClose();
        } else {
            remoteState = LinkState.DETACHED;
            operability = LinkOperabilityState.LINK_REMOTELY_DETACHED;
            transitionToRemotelyDetached();
            fireRemoteDetach();
        }

        return this;
    }

    final ProtonIncomingDelivery remoteTransfer(Transfer transfer, ProtonBuffer payload) {
        LOG.trace("Link:{} Received new Transfer:{}", self(), transfer);
        return handleRemoteTransfer(transfer, payload);
    }

    final T remoteFlow(Flow flow) {
        LOG.trace("Link:{} Received new Flow:{}", self(), flow);
        return handleRemoteFlow(flow);
    }

    final T remoteDisposition(Disposition disposition, ProtonOutgoingDelivery delivery) {
        LOG.trace("Link:{} Received remote disposition:{} for sent delivery:{}", self(), disposition, delivery);
        return handleRemoteDisposition(disposition, delivery);
    }

    final T remoteDisposition(Disposition disposition, ProtonIncomingDelivery delivery) {
        LOG.trace("Link:{} Received remote disposition:{} for received delivery:{}", self(), disposition, delivery);
        return handleRemoteDisposition(disposition, delivery);
    }

    //----- Abstract methods required for specialization of the link type

    protected abstract T handleRemoteAttach(Attach attach);

    protected abstract T handleRemoteDetach(Detach detach);

    protected abstract T handleRemoteFlow(Flow flow);

    protected abstract T handleRemoteDisposition(Disposition disposition, ProtonOutgoingDelivery delivery);

    protected abstract T handleRemoteDisposition(Disposition disposition, ProtonIncomingDelivery delivery);

    protected abstract ProtonIncomingDelivery handleRemoteTransfer(Transfer transfer, ProtonBuffer payload);

    protected abstract T decorateOutgoingFlow(Flow flow);

    //----- Internal methods

    ProtonLinkCreditState getCreditState() {
        return creditState;
    }

    boolean wasLocalAttachSent() {
        return localAttachSent;
    }

    boolean wasLocalDetachSent() {
        return localDetachSent;
    }

    void trySyncLocalStateWithRemote() {
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

    private void trySendLocalAttach() {
        if (!wasLocalAttachSent()) {
            if ((session.isLocallyOpen() && session.wasLocalBeginSent()) &&
                (connection.isLocallyOpen() && connection.wasLocalOpenSent())) {

                localAttachSent = true;
                session.getEngine().fireWrite(localAttach, session.getLocalChannel(), null, null);

                if (isLocallyOpen() && isReceiver() && getCreditState().hasCredit()) {
                    session.writeFlow(this);
                }
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

    protected void checkLinkOperable(String failurePrefix) {
        switch (operability) {
            case OK:
                break;
            case ENGINE_FAILED:
                throw new EngineFailedException(failurePrefix + ": Engine Failed", engine.failureCause());
            default:
                throw new IllegalStateException(failurePrefix + ": " + operability.toString());
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
}
