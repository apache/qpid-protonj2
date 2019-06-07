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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Close;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.End;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;
import org.apache.qpid.proton4j.engine.AsyncEvent;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.ConnectionState;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;
import org.apache.qpid.proton4j.engine.SessionState;
import org.apache.qpid.proton4j.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.proton4j.engine.exceptions.ProtonException;

/**
 * Implements the proton4j Connection API
 */
public class ProtonConnection implements Connection, AMQPHeader.HeaderHandler<ProtonEngine>, Performative.PerformativeHandler<ProtonEngine> {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonConnection.class);

    private final ProtonEngine engine;

    private final Open localOpen = new Open();
    private Open remoteOpen;

    private Map<Integer, ProtonSession> localSessions = new HashMap<>();
    private Map<Integer, ProtonSession> remoteSessions = new HashMap<>();

    private final ProtonContext context = new ProtonContext();

    private ConnectionState localState = ConnectionState.IDLE;
    private ConnectionState remoteState = ConnectionState.IDLE;

    private ErrorCondition localError;
    private ErrorCondition remoteError;

    private boolean headerSent;
    private boolean localOpenSent;
    private boolean localCloseSent;

    private EventHandler<AsyncEvent<Connection>> remoteOpenHandler = (result) -> {
        LOG.trace("Remote open arrived at default handler.");
    };

    private EventHandler<AsyncEvent<Connection>> remoteCloseHandler = (result) -> {
        LOG.trace("Remote close arrived at default handler.");
    };

    private EventHandler<Session> remoteSessionOpenEventHandler = (result) -> {
        LOG.trace("Remote session open arrived at default handler.");
    };

    private EventHandler<Sender> remoteSenderOpenEventHandler = (result) -> {
        LOG.trace("Remote sender open arrived at default handler.");
    };

    private EventHandler<Receiver> remoteReceiverOpenEventHandler = (result) -> {
        LOG.trace("Remote receiver open arrived at default handler.");
    };

    /**
     * Create a new unbound Connection instance.
     *
     * @param engine
     */
    ProtonConnection(ProtonEngine engine) {
        this.engine = engine;

        // Base the initial max frame size on the value configured on the engine.
        // this.localOpen.setMaxFrameSize(engine.configuration().getMaxFrameSize());
        // TODO - This creates a default which we haven't settled on so leaving it off for now.
    }

    public ProtonEngine getEngine() {
        return engine;
    }

    @Override
    public ProtonContext getContext() {
        return context;
    }

    @Override
    public ConnectionState getLocalState() {
        return localState;
    }

    @Override
    public ErrorCondition getLocalCondition() {
        return localError;
    }

    @Override
    public ProtonConnection setLocalCondition(ErrorCondition condition) {
        localError = condition == null ? null : condition.copy();
        return this;
    }

    @Override
    public ProtonConnection open() {
        if (getLocalState() == ConnectionState.IDLE) {
            localState = ConnectionState.ACTIVE;
            processStateChangeAndRespond();
        }

        return this;
    }

    @Override
    public ProtonConnection close() {
        if (getLocalState() == ConnectionState.ACTIVE) {
            localState = ConnectionState.CLOSED;
            processStateChangeAndRespond();
        }

        return this;
    }

    @Override
    public ProtonConnection setContainerId(String containerId) {
        checkNotOpened("Cannot set Container Id on already opened Connection");
        localOpen.setContainerId(containerId);
        return this;
    }

    @Override
    public String getContainerId() {
        return localOpen.getContainerId();
    }

    @Override
    public ProtonConnection setHostname(String hostname) {
        checkNotOpened("Cannot set Hostname on already opened Connection");
        localOpen.setHostname(hostname);
        return this;
    }

    @Override
    public String getHostname() {
        return localOpen.getHostname();
    }

    @Override
    public Connection setMaxFrameSize(long maxFrameSize) {
        checkNotOpened("Cannot set Max Frame Size on already opened Connection");
        localOpen.setMaxFrameSize(maxFrameSize);
        engine.configuration().setMaxFrameSize((int) maxFrameSize);
        return this;
    }

    @Override
    public long getMaxFrameSize() {
        return localOpen.getMaxFrameSize();
    }

    @Override
    public ProtonConnection setChannelMax(int channelMax) {
        checkNotOpened("Cannot set Channel Max on already opened Connection");
        localOpen.setChannelMax(channelMax);
        return this;
    }

    @Override
    public int getChannelMax() {
        return localOpen.getChannelMax();
    }

    @Override
    public ProtonConnection setIdleTimeout(int idleTimeout) {
        checkNotOpened("Cannot set Idle Timeout on already opened Connection");
        localOpen.setIdleTimeOut(idleTimeout);
        return this;
    }

    @Override
    public int getIdleTimeout() {
        return (int) localOpen.getIdleTimeOut();
    }

    @Override
    public ProtonConnection setOfferedCapabilities(Symbol[] capabilities) {
        checkNotOpened("Cannot set Offered Capabilities on already opened Connection");

        if (capabilities != null) {
            localOpen.setOfferedCapabilities(Arrays.copyOf(capabilities, capabilities.length));
        } else {
            localOpen.setOfferedCapabilities(capabilities);
        }

        return this;
    }

    @Override
    public Symbol[] getOfferedCapabilities() {
        if (localOpen.getOfferedCapabilities() != null) {
            return Arrays.copyOf(localOpen.getOfferedCapabilities(), localOpen.getOfferedCapabilities().length);
        }

        return null;
    }

    @Override
    public ProtonConnection setDesiredCapabilities(Symbol[] capabilities) {
        checkNotOpened("Cannot set Desired Capabilities on already opened Connection");

        if (capabilities != null) {
            localOpen.setDesiredCapabilities(Arrays.copyOf(capabilities, capabilities.length));
        } else {
            localOpen.setDesiredCapabilities(capabilities);
        }

        return this;
    }

    @Override
    public Symbol[] getDesiredCapabilities() {
        if (localOpen.getDesiredCapabilities() != null) {
            return Arrays.copyOf(localOpen.getDesiredCapabilities(), localOpen.getDesiredCapabilities().length);
        }

        return null;
    }

    @Override
    public ProtonConnection setProperties(Map<Symbol, Object> properties) {
        checkNotOpened("Cannot set Properties on already opened Connection");

        if (properties != null) {
            localOpen.setProperties(new LinkedHashMap<>(properties));
        } else {
            localOpen.setProperties(properties);
        }

        return this;
    }

    @Override
    public Map<Symbol, Object> getProperties() {
        if (localOpen.getProperties() != null) {
            return Collections.unmodifiableMap(localOpen.getProperties());
        }

        return null;
    }

    @Override
    public String getRemoteContainerId() {
        return remoteOpen == null ? null : remoteOpen.getContainerId();
    }

    @Override
    public String getRemoteHostname() {
        return remoteOpen == null ? null : remoteOpen.getHostname();
    }

    @Override
    public Symbol[] getRemoteOfferedCapabilities() {
        if (remoteOpen != null && remoteOpen.getOfferedCapabilities() != null) {
            return Arrays.copyOf(remoteOpen.getOfferedCapabilities(), remoteOpen.getOfferedCapabilities().length);
        }

        return null;
    }

    @Override
    public Symbol[] getRemoteDesiredCapabilities() {
        if (remoteOpen != null && remoteOpen.getDesiredCapabilities() != null) {
            return Arrays.copyOf(remoteOpen.getDesiredCapabilities(), remoteOpen.getDesiredCapabilities().length);
        }

        return null;
    }

    @Override
    public Map<Symbol, Object> getRemoteProperties() {
        if (remoteOpen != null && remoteOpen.getProperties() != null) {
            return Collections.unmodifiableMap(remoteOpen.getProperties());
        }

        return null;
    }

    @Override
    public ConnectionState getRemoteState() {
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
    public ProtonSession session() {
        checkConnectionClosed("Cannot create a Session from a Connection that is already closed");

        int localChannel = findFreeLocalChannel();
        ProtonSession newSession = new ProtonSession(this, localChannel);
        localSessions.put(localChannel, newSession);

        return newSession;
    }

    //----- Handle performatives sent from the remote to this Connection

    @Override
    public void handleAMQPHeader(AMQPHeader header, ProtonEngine context) {
        processStateChangeAndRespond();
    }

    @Override
    public void handleSASLHeader(AMQPHeader header, ProtonEngine context) {
        context.engineFailed(new ProtonException("Receivded unexpected SASL Header"));
    }

    @Override
    public void handleOpen(Open open, ProtonBuffer payload, int channel, ProtonEngine context) {
        if (remoteOpen != null) {
            context.engineFailed(new ProtocolViolationException("Received second Open for Connection from remote"));
        }

        remoteState = ConnectionState.ACTIVE;
        remoteOpen = open;

        // Inform the local sessions that remote has opened this connection.
        for (ProtonSession localSession : localSessions.values()) {
            localSession.remoteOpen(open, channel);
        }

        if (remoteOpenHandler != null) {
            remoteOpenHandler.handle(result(this, null));
        }
    }

    @Override
    public void handleClose(Close close, ProtonBuffer payload, int channel, ProtonEngine context) {
        remoteState = ConnectionState.CLOSED;
        setRemoteCondition(close.getError());

        // Inform the local sessions that remote has closed this connection.
        for (ProtonSession session : localSessions.values()) {
            session.remoteClose(close, channel);
        }

        if (remoteCloseHandler != null) {
            remoteCloseHandler.handle(result(this, close.getError()));
        }
    }

    @Override
    public void handleBegin(Begin begin, ProtonBuffer payload, int channel, ProtonEngine context) {
        ProtonSession session = null;

        if (channel > localOpen.getChannelMax()) {
            // TODO Channel Max violation error handling
        }

        if (remoteSessions.containsKey(channel)) {
            context.engineFailed(new ProtocolViolationException("Received second begin for Session from remote"));
        } else {
            // If there is a remote channel then this is an answer to a local open of a session, otherwise
            // the remote is requesting a new session and we need to create one and signal that a remote
            // session was opened.
            if (begin.hasRemoteChannel()) {
                int remoteChannel = begin.getRemoteChannel();
                session = localSessions.get(begin.getRemoteChannel());
                if (session == null) {
                    // TODO What should be the correct response to this particular wrinkle
                    engine.engineFailed(new ProtocolViolationException("Received uncorrelated channel on Begin from remote: " + remoteChannel));
                    return;
                }
            } else {
                session = session();
            }

            remoteSessions.put(channel, session);

            // Let the session handle the remote Begin now.
            session.remoteBegin(begin, channel);

            // If the session was initiated remotely then we signal the creation to the any registered
            // remote session event handler
            if (session.getLocalState() == SessionState.IDLE && remoteSessionOpenEventHandler != null) {
                remoteSessionOpenEventHandler.handle(session);
            }
        }
    }

    @Override
    public void handleEnd(End end, ProtonBuffer payload, int channel, ProtonEngine context) {
        final ProtonSession session = remoteSessions.remove(channel);
        if (session == null) {
            engine.engineFailed(new ProtocolViolationException("Received uncorrelated channel on End from remote: " + channel));
        }

        session.remoteEnd(end, channel);
    }

    @Override
    public void handleAttach(Attach attach, ProtonBuffer payload, int channel, ProtonEngine context) {
        final ProtonSession session = remoteSessions.get(channel);
        if (session == null) {
            engine.engineFailed(new ProtocolViolationException("Received uncorrelated channel on Attach from remote: " + channel));
        }

        session.remoteAttach(attach, channel);
    }

    @Override
    public void handleDetach(Detach detach, ProtonBuffer payload, int channel, ProtonEngine context) {
        final ProtonSession session = remoteSessions.get(channel);
        if (session == null) {
            engine.engineFailed(new ProtocolViolationException("Received uncorrelated channel on Detach from remote: " + channel));
        }

        session.remoteDetach(detach, channel);
    }

    @Override
    public void handleFlow(Flow flow, ProtonBuffer payload, int channel, ProtonEngine context) {
        final ProtonSession session = remoteSessions.get(channel);
        if (session == null) {
            engine.engineFailed(new ProtocolViolationException("Received uncorrelated channel on Flow from remote: " + channel));
        }

        session.remoteFlow(flow, channel);
    }

    @Override
    public void handleTransfer(Transfer transfer, ProtonBuffer payload, int channel, ProtonEngine context) {
        final ProtonSession session = remoteSessions.get(channel);
        if (session == null) {
            engine.engineFailed(new ProtocolViolationException("Received uncorrelated channel on Transfer from remote: " + channel));
        }

        session.remoteTransfer(transfer, payload, channel);
    }

    @Override
    public void handleDisposition(Disposition disposition, ProtonBuffer payload, int channel, ProtonEngine context) {
        final ProtonSession session = remoteSessions.get(channel);
        if (session == null) {
            engine.engineFailed(new ProtocolViolationException("Received uncorrelated channel on Disposition from remote: " + channel));
        }

        session.remoteDispsotion(disposition, channel);
    }

    //----- API for event handling of Connection related remote events

    @Override
    public Connection openEventHandler(EventHandler<AsyncEvent<Connection>> remoteOpenEventHandler) {
        this.remoteOpenHandler = remoteOpenEventHandler;
        return this;
    }

    @Override
    public Connection closeEventHandler(EventHandler<AsyncEvent<Connection>> remoteCloseEventHandler) {
        this.remoteCloseHandler = remoteCloseEventHandler;
        return this;
    }

    @Override
    public Connection sessionOpenEventHandler(EventHandler<Session> remoteSessionOpenEventHandler) {
        this.remoteSessionOpenEventHandler = remoteSessionOpenEventHandler;
        return this;
    }

    @Override
    public Connection senderOpenEventHandler(EventHandler<Sender> remoteSenderOpenEventHandler) {
        this.remoteSenderOpenEventHandler = remoteSenderOpenEventHandler;
        return this;
    }

    EventHandler<Sender> senderOpenEventHandler() {
        return remoteSenderOpenEventHandler;
    }

    @Override
    public Connection receiverOpenEventHandler(EventHandler<Receiver> remoteReceiverOpenEventHandler) {
        this.remoteReceiverOpenEventHandler = remoteReceiverOpenEventHandler;
        return this;
    }

    EventHandler<Receiver> receiverOpenEventHandler() {
        return remoteReceiverOpenEventHandler;
    }

    //----- Internal implementation

    private void checkNotOpened(String errorMessage) {
        if (localState.ordinal() > ConnectionState.IDLE.ordinal()) {
            throw new IllegalStateException(errorMessage);
        }
    }

    private void checkConnectionClosed(String errorMessage) {
        if (isLocallyClosed() || isRemotelyClosed()) {
             throw new IllegalStateException(errorMessage);
        }
    }

    private void processStateChangeAndRespond() {
        // When the engine state changes or we have read an incoming AMQP header etc we need to check
        // if we have pending work to send and do so
        if (headerSent) {
            // Once an incoming header arrives we can emit our open if locally opened and also send close if
            // that is what our state is already.
            if (getLocalState() != ConnectionState.IDLE) {
                if (!localOpenSent) {
                    localOpenSent = true;
                    engine.pipeline().fireWrite(localOpen, 0, null, null);
                    engine.configuration().recomputeEffectiveFrameSizeLimits();
                }

                if (getLocalState() == ConnectionState.CLOSED && !localCloseSent) {
                    localCloseSent = true;
                    Close localClose = new Close().setError(getLocalCondition());
                    // Inform all sessions that the connection has now written its local close
                    ArrayList<ProtonSession> sessions = new ArrayList<>(localSessions.values());
                    sessions.forEach(session -> {
                        session.localClose(localClose);
                    });
                    engine.pipeline().fireWrite(localClose, 0, null, null);
                } else {
                    // Inform all sessions that the connection has now written its local open
                    ArrayList<ProtonSession> sessions = new ArrayList<>(localSessions.values());
                    sessions.forEach(session -> {
                        session.localOpen(localOpen);
                    });
                }
            }
        } else {
            headerSent = true;
            engine.pipeline().fireWrite(AMQPHeader.getAMQPHeader());
        }
    }

    private int findFreeLocalChannel() {
        for (int i = 0; i < ProtonConstants.CHANNEL_MAX; ++i) {
            if (!localSessions.containsKey(i)) {
                return i;
            }
        }

        throw new IllegalStateException("no local channel available for allocation");
    }

    void freeLocalChannel(int localChannel) {
        if (localChannel > ProtonConstants.CHANNEL_MAX) {
            throw new IllegalArgumentException("Specified local channel is out of range: " + localChannel);
        }

        localSessions.remove(localChannel);
    }

    boolean isLocallyOpened() {
        return getLocalState() == ConnectionState.ACTIVE;
    }

    boolean isRemotelyOpened() {
        return getRemoteState() == ConnectionState.ACTIVE;
    }

    boolean isLocallyClosed() {
        return getLocalState() == ConnectionState.CLOSED;
    }

    boolean isRemotelyClosed() {
        return getRemoteState() == ConnectionState.CLOSED;
    }

    boolean wasHeaderSent() {
        return this.headerSent;
    }

    boolean wasLocalOpenSent() {
        return this.localOpenSent;
    }

    boolean wasLocalCloseSent() {
        return this.localCloseSent;
    }
}
