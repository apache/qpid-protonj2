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

import static org.apache.qpid.proton4j.engine.impl.ProtonSupport.result;

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
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;
import org.apache.qpid.proton4j.engine.AsyncEvent;
import org.apache.qpid.proton4j.engine.ConnectionState;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.Link;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;

/**
 * Common base for Proton Senders and Receivers.
 *
 * @param <T> the type of link, {@link Sender} or {@link Receiver}.
 */
public abstract class ProtonLink<T extends Link<T>> implements Link<T>, Performative.PerformativeHandler<ProtonEngine> {

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

    private long remoteDeliveryCount;
    private long remoteLinkCredit;

    private EventHandler<AsyncEvent<T>> remoteOpenHandler;

    private EventHandler<AsyncEvent<T>> remoteDetachHandler = (result) -> {
        LOG.trace("Remote link detach arrived at default handler.");
    };
    private EventHandler<AsyncEvent<T>> remoteCloseHandler = (result) -> {
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
    ProtonLink(ProtonSession session, String name) {
        this.session = session;
        this.connection = session.getConnection();
        this.localAttach.setName(name);
        this.localAttach.setRole(getRole());
    }

    @Override
    public Session getSession() {
        return session;
    }

    @Override
    public String getName() {
        return localAttach.getName();
    }

    protected abstract T self();

    @Override
    public ProtonContext getContext() {
        return context;
    }

    @Override
    public LinkState getLocalState() {
        return localState;
    }

    @Override
    public ErrorCondition getLocalCondition() {
        return localError;
    }

    @Override
    public ProtonLink<T> setLocalCondition(ErrorCondition condition) {
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
        if (getLocalState() == LinkState.IDLE) {
            localState = LinkState.ACTIVE;
            long localHandle = session.findFreeLocalHandle();
            localAttach.setHandle(localHandle);
            session.getEngine().pipeline().fireWrite(localAttach, session.getLocalChannel(), null, null);
        }

        return this;
    }

    @Override
    public ProtonLink<T> detach() {
        if (getLocalState() == LinkState.ACTIVE) {
            localState = LinkState.DETACHED;
            // TODO - Additional processing.
            Detach detach = new Detach();
            detach.setHandle(localAttach.getHandle());
            detach.setClosed(false);
            detach.setError(getLocalCondition());

            session.getEngine().pipeline().fireWrite(detach, session.getLocalChannel(), null, null);
            session.freeLocalHandle(localAttach.getHandle());
        }

        return this;
    }

    @Override
    public ProtonLink<T> close() {
        if (getLocalState() == LinkState.ACTIVE) {
            localState = LinkState.CLOSED;
            // TODO - Additional processing.
            Detach detach = new Detach();
            detach.setHandle(localAttach.getHandle());
            detach.setClosed(true);
            detach.setError(getLocalCondition());

            session.getEngine().pipeline().fireWrite(detach, session.getLocalChannel(), null, null);
            session.freeLocalHandle(localAttach.getHandle());
        }

        return this;
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
    public T openHandler(EventHandler<AsyncEvent<T>> remoteOpenHandler) {
        this.remoteOpenHandler = remoteOpenHandler;
        return self();
    }

    @Override
    public T detachHandler(EventHandler<AsyncEvent<T>> remoteDetachHandler) {
        this.remoteDetachHandler = remoteDetachHandler;
        return self();
    }

    @Override
    public T closeHandler(EventHandler<AsyncEvent<T>> remoteCloseHandler) {
        this.remoteCloseHandler = remoteCloseHandler;
        return self();
    }

    //----- Handle incoming performatives

    @Override
    public void handleBegin(Begin begin, ProtonBuffer payload, int channel, ProtonEngine context) {
        // TODO - Possible that we handle begin and send frames based on session state and state of this link
    }

    @Override
    public void handleAttach(Attach attach, ProtonBuffer payload, int channel, ProtonEngine context) {
        remoteAttach = attach;
        remoteState = LinkState.ACTIVE;
        remoteDeliveryCount = attach.getInitialDeliveryCount();

        if (remoteOpenHandler != null) {
            remoteOpenHandler.handle(result(self(), null));
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

    @Override
    public void handleDetach(Detach detach, ProtonBuffer payload, int channel, ProtonEngine context) {
        setRemoteCondition(detach.getError());

        if (detach.getClosed()) {
            remoteState = LinkState.CLOSED;
            if (remoteCloseHandler != null) {
                remoteCloseHandler.handle(result(self(), getRemoteCondition()));
            }
        } else {
            remoteState = LinkState.DETACHED;
            if (remoteDetachHandler != null) {
                remoteDetachHandler.handle(result(self(), getRemoteCondition()));
            }
        }
    }

    @Override
    public void handleFlow(Flow flow, ProtonBuffer payload, int channel, ProtonEngine context) {
        remoteDeliveryCount = flow.getDeliveryCount();
        remoteLinkCredit = flow.getLinkCredit();
    }

    //----- Internal handler methods

    long getRemoteDeliveryCount() {
        return remoteDeliveryCount;
    }

    long getRemoteLinkCredit() {
        return remoteLinkCredit;
    }

    private void checkNotOpened(String errorMessage) {
        if (localState.ordinal() > ConnectionState.IDLE.ordinal()) {
            throw new IllegalStateException(errorMessage);
        }
    }
}
