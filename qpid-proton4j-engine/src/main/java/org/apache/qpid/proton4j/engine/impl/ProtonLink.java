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
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Close;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.End;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Role;
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

    protected final Attach localAttach = new Attach();
    protected Attach remoteAttach;

    private final ProtonContext context = new ProtonContext();

    private LinkState localState = LinkState.IDLE;
    private LinkState remoteState = LinkState.IDLE;

    private ErrorCondition localError;
    private ErrorCondition remoteError;

    private EventHandler<T> remoteOpenHandler;

    private EventHandler<T> remoteDetachHandler = (result) -> {
        LOG.trace("Remote link detach arrived at default handler.");
    };
    private EventHandler<T> remoteCloseHandler = (result) -> {
        LOG.trace("Remote link close arrived at default handler.");
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
        this.localAttach.setName(name);
        this.localAttach.setRole(getRole());
    }

    @Override
    public ProtonSession getSession() {
        return session;
    }

    @Override
    public String getName() {
        return localAttach.getName();
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
    public ProtonLink<T> setCondition(ErrorCondition condition) {
        localError = condition == null ? null : condition.copy();

        return this;
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
    public ProtonLink<T> open() {
        checkSessionNotClosed();
        if (getState() == LinkState.IDLE) {
            localState = LinkState.ACTIVE;
            long localHandle = session.findFreeLocalHandle(this);
            localAttach.setHandle(localHandle);
            transitionedToLocallyOpened();
            trySendLocalAttach();
        }

        return this;
    }

    protected void transitionedToLocallyOpened() {
        // Subclass can respond to this state change as needed.
    }

    @Override
    public ProtonLink<T> detach() {
        if (getState() == LinkState.ACTIVE) {
            localState = LinkState.DETACHED;
            transitionedToLocallyDetached();
            trySendLocalDetach(false);
        }

        return this;
    }

    protected void transitionedToLocallyDetached() {
        // Subclass can respond to this state change as needed.
    }

    @Override
    public ProtonLink<T> close() {
        if (getState() == LinkState.ACTIVE) {
            localState = LinkState.CLOSED;
            transitionedToLocallyClosed();
            trySendLocalDetach(true);
        }

        return this;
    }

    protected void transitionedToLocallyClosed() {
        // Subclass can respond to this state change as needed.
    }

    @Override
    public ProtonLink<T> setSource(Source source) {
        checkNotOpened("Cannot set Source on already opened Link");
        localAttach.setSource(source);
        return this;
    }

    @Override
    public Source getSource() {
        return localAttach.getSource();
    }

    @Override
    public ProtonLink<T> setTarget(Target target) {
        checkNotOpened("Cannot set Target on already opened Link");
        localAttach.setTarget(target);
        return this;
    }

    @Override
    public Target getTarget() {
        return localAttach.getTarget();
    }

    @Override
    public ProtonLink<T> setProperties(Map<Symbol, Object> properties) {
        checkNotOpened("Cannot set Properties on already opened Link");

        if (properties != null) {
            localAttach.setProperties(new LinkedHashMap<>(properties));
        } else {
            localAttach.setProperties(properties);
        }

        return this;
    }

    @Override
    public Map<Symbol, Object> getProperties() {
        if (localAttach.getProperties() != null) {
            return Collections.unmodifiableMap(localAttach.getProperties());
        }

        return null;
    }

    @Override
    public ProtonLink<T> setOfferedCapabilities(Symbol[] capabilities) {
        checkNotOpened("Cannot set Offered Capabilities on already opened Link");

        if (capabilities != null) {
            localAttach.setOfferedCapabilities(Arrays.copyOf(capabilities, capabilities.length));
        } else {
            localAttach.setOfferedCapabilities(capabilities);
        }

        return this;
    }

    @Override
    public Symbol[] getOfferedCapabilities() {
        if (localAttach.getOfferedCapabilities() != null) {
            return Arrays.copyOf(localAttach.getOfferedCapabilities(), localAttach.getOfferedCapabilities().length);
        }

        return null;
    }

    @Override
    public ProtonLink<T> setDesiredCapabilities(Symbol[] capabilities) {
        checkNotOpened("Cannot set Desired Capabilities on already opened Link");

        if (capabilities != null) {
            localAttach.setDesiredCapabilities(Arrays.copyOf(capabilities, capabilities.length));
        } else {
            localAttach.setDesiredCapabilities(capabilities);
        }

        return this;
    }

    @Override
    public Symbol[] getDesiredCapabilities() {
        if (localAttach.getDesiredCapabilities() != null) {
            return Arrays.copyOf(localAttach.getDesiredCapabilities(), localAttach.getDesiredCapabilities().length);
        }

        return null;
    }

    @Override
    public ProtonLink<T> setMaxMessageSize(UnsignedLong maxMessageSize) {
        checkNotOpened("Cannot set Max Message Size on already opened Link");
        localAttach.setMaxMessageSize(maxMessageSize);
        return this;
    }

    @Override
    public UnsignedLong getMaxMessageSize() {
        return localAttach.getMaxMessageSize();
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

    //----- Process local events from the parent session

    void localBegin(Begin begin, int channel) {
        // Fire held attach if link already marked as active.
        if (getState().ordinal() >= LinkState.ACTIVE.ordinal()) {
            trySendLocalAttach();
        }

        // If already closed or detached this is the time to send that along as well.
        if (isLocallyDetached()) {
            trySendLocalDetach(false);
        } else if (isLocallyClosed()) {
            trySendLocalDetach(true);
        }
    }

    void localClose(Close localClose) {
        if (!isLocallyClosed()) {
            // TODO - State as closed or detached ?
            localState = LinkState.CLOSED;
            linkState().localClose(true);
            transitionedToLocallyClosed();
        }
    }

    void localEnd(End end, int channel) {
        if (!isLocallyClosed()) {
            // TODO - State as closed or detached ?
            localState = LinkState.CLOSED;
            linkState().localClose(true);
            transitionedToLocallyClosed();
        }
    }

    //----- Handle incoming performatives

    void remoteAttach(Attach attach) {
        remoteAttach = attach;
        remoteState = LinkState.ACTIVE;
        linkState().remoteAttach(attach);

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

        if (detach.getClosed()) {
            remoteState = LinkState.CLOSED;
            if (remoteCloseHandler != null) {
                remoteCloseHandler.handle(self());
            }
        } else {
            remoteState = LinkState.DETACHED;
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

    boolean isLocallyOpened() {
        return getState() == LinkState.ACTIVE;
    }

    boolean isRemotelyOpened() {
        return getRemoteState() == LinkState.ACTIVE;
    }

    boolean isLocallyClosed() {
        return getState() == LinkState.CLOSED;
    }

    boolean isRemotelyClosed() {
        return getRemoteState() == LinkState.CLOSED;
    }

    boolean isLocallyDetached() {
        return getState() == LinkState.DETACHED;
    }

    boolean isRemotelyDetached() {
        return getRemoteState() == LinkState.DETACHED;
    }

    private void trySendLocalAttach() {
        if ((session.isLocallyOpened() && session.wasLocalBeginSent()) &&
            (connection.isLocallyOpened() && connection.wasLocalOpenSent())) {

            // TODO - Still need to check for transport being writable at this time.
            session.getEngine().pipeline().fireWrite(
                linkState().configureAttach(localAttach), session.getLocalChannel(), null, null);
        }
    }

    private void trySendLocalDetach(boolean closed) {
        if ((session.isLocallyOpened() && session.wasLocalBeginSent()) &&
            (connection.isLocallyOpened() && connection.wasLocalOpenSent())) {

            // TODO - Still need to check that transport is writable
            Detach detach = new Detach();
            detach.setHandle(localAttach.getHandle());
            detach.setClosed(closed);
            detach.setError(getCondition());

            session.freeLocalHandle(localAttach.getHandle());
            session.getEngine().pipeline().fireWrite(detach, session.getLocalChannel(), null, null);
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
}
