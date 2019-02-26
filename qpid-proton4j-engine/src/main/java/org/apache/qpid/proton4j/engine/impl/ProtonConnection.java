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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
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

    private static final int CHANNEL_MAX = 65535;

    private final ProtonEngine engine;

    private final Open localOpen = new Open();
    private Open remoteOpen;

    private Map<Integer, ProtonSession> localSessions = new HashMap<Integer, ProtonSession>();
    private Map<Integer, ProtonSession> remoteSessions = new HashMap<Integer, ProtonSession>();

    private Object context;
    private Map<String, Object> contextEntries = new HashMap<>();

    private ConnectionState localState = ConnectionState.IDLE;
    private ConnectionState remoteState = ConnectionState.IDLE;

    private ErrorCondition localError = new ErrorCondition();
    private ErrorCondition remoteError = new ErrorCondition();

    private boolean headerReceived;
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
     */
    public ProtonConnection(ProtonEngine engine) {
        this.engine = engine;
    }

    public ProtonEngine getEngine() {
        return engine;
    }

    @Override
    public void setContext(Object context) {
        this.context = context;
    }

    @Override
    public Object getContext() {
        return context;
    }

    @Override
    public void setContextEntry(String key, Object value) {
        contextEntries.put(key, value);
    }

    @Override
    public Object getContextEntry(String key) {
        return contextEntries.get(key);
    }

    @Override
    public ConnectionState getLocalState() {
        return localState;
    }

    @Override
    public ErrorCondition getLocalCondition() {
        return localError.isEmpty() ? null : localError;
    }

    @Override
    public void setLocalCondition(ErrorCondition condition) {
        if (condition != null) {
            localError = condition.copy();
        } else {
            localError.clear();
        }
    }

    @Override
    public void open() {
        if (getLocalState() == ConnectionState.IDLE) {
            localState = ConnectionState.ACTIVE;
            processStateChangeAndRespond();
        }
    }

    @Override
    public void close() {
        if (getLocalState() == ConnectionState.ACTIVE) {
            localState = ConnectionState.CLOSED;
            processStateChangeAndRespond();
        }
    }

    @Override
    public void setContainerId(String containerId) {
        checkNotOpened("Cannot set Container Id on already opened Connection");
        localOpen.setContainerId(containerId);
    }

    @Override
    public String getContainerId() {
        return localOpen.getContainerId();
    }

    @Override
    public void setHostname(String hostname) {
        checkNotOpened("Cannot set Hostname on already opened Connection");
        localOpen.setHostname(hostname);
    }

    @Override
    public String getHostname() {
        return localOpen.getHostname();
    }

    @Override
    public void setChannelMax(int channelMax) {
        checkNotOpened("Cannot set Channel Max on already opened Connection");
        localOpen.setChannelMax(UnsignedShort.valueOf((short) channelMax));
    }

    @Override
    public int getChannelMax() {
        return localOpen.getChannelMax().intValue();
    }

    @Override
    public void setIdleTimeout(int idleTimeout) {
        checkNotOpened("Cannot set Idle Timeout on already opened Connection");
        localOpen.setIdleTimeOut(UnsignedInteger.valueOf(idleTimeout));
    }

    @Override
    public int getIdleTimeout() {
        return localOpen.getIdleTimeOut().intValue();
    }

    @Override
    public void setOfferedCapabilities(Symbol[] capabilities) {
        checkNotOpened("Cannot set Offered Capabilities on already opened Connection");

        if (capabilities != null) {
            localOpen.setOfferedCapabilities(Arrays.copyOf(capabilities, capabilities.length));
        } else {
            localOpen.setOfferedCapabilities(capabilities);
        }
    }

    @Override
    public Symbol[] getOfferedCapabilities() {
        if (localOpen.getOfferedCapabilities() != null) {
            return Arrays.copyOf(localOpen.getOfferedCapabilities(), localOpen.getOfferedCapabilities().length);
        }

        return null;
    }

    @Override
    public void setDesiredCapabilities(Symbol[] capabilities) {
        checkNotOpened("Cannot set Desired Capabilities on already opened Connection");

        if (capabilities != null) {
            localOpen.setDesiredCapabilities(Arrays.copyOf(capabilities, capabilities.length));
        } else {
            localOpen.setDesiredCapabilities(capabilities);
        }
    }

    @Override
    public Symbol[] getDesiredCapabilities() {
        if (localOpen.getDesiredCapabilities() != null) {
            return Arrays.copyOf(localOpen.getDesiredCapabilities(), localOpen.getDesiredCapabilities().length);
        }

        return null;
    }

    @Override
    public void setProperties(Map<Symbol, Object> properties) {
        checkNotOpened("Cannot set Properties on already opened Connection");

        if (properties != null) {
            localOpen.setProperties(new LinkedHashMap<>(properties));
        } else {
            localOpen.setProperties(properties);
        }
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
        if (condition != null) {
            remoteError = condition.copy();
        } else {
            remoteError.clear();
        }
    }

    @Override
    public ProtonSession session() {
        int localChannel = findFreeLocalChannel();
        ProtonSession newSession = new ProtonSession(this, localChannel);
        localSessions.put(localChannel, newSession);

        return newSession;
    }

    //----- Handle performatives sent from the remote to this Connection

    @Override
    public void handleAMQPHeader(AMQPHeader header, ProtonEngine context) {
        headerReceived = true;
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

        // TODO - Inform all Sessions that the remote has opened ?
        // foreach session -> open.invoke(session, payload, channel, context);

        remoteOpenHandler.handle(result(this, null));
    }

    @Override
    public void handleClose(Close close, ProtonBuffer payload, int channel, ProtonEngine context) {
        remoteState = ConnectionState.CLOSED;
        setRemoteCondition(close.getError());

        // TODO - Inform all Sessions that the remote has closed ?
        // foreach session -> close.invoke(session, payload, channel, context);

        remoteCloseHandler.handle(result(this, close.getError()));
    }

    @Override
    public void handleBegin(Begin begin, ProtonBuffer payload, int channel, ProtonEngine context) {
        ProtonSession session = null;

        if (channel > localOpen.getChannelMax().intValue()) {
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
            begin.invoke(session, payload, channel, context);

            // If the session was initiated remotely then we signal the creation to the any registered
            // remote session event handler
            if (session.getLocalState() == SessionState.IDLE) {
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

        end.invoke(session, payload, channel, context);
    }

    @Override
    public void handleAttach(Attach attach, ProtonBuffer payload, int channel, ProtonEngine context) {
        final ProtonSession session = remoteSessions.get(channel);
        if (session == null) {
            engine.engineFailed(new ProtocolViolationException("Received uncorrelated channel on Attach from remote: " + channel));
        }

        attach.invoke(session, payload, channel, context);
    }

    @Override
    public void handleDetach(Detach detach, ProtonBuffer payload, int channel, ProtonEngine context) {
        final ProtonSession session = remoteSessions.get(channel);
        if (session == null) {
            engine.engineFailed(new ProtocolViolationException("Received uncorrelated channel on Detach from remote: " + channel));
        }

        detach.invoke(session, payload, channel, context);
    }

    @Override
    public void handleFlow(Flow flow, ProtonBuffer payload, int channel, ProtonEngine context) {
        final ProtonSession session = remoteSessions.get(channel);
        if (session == null) {
            engine.engineFailed(new ProtocolViolationException("Received uncorrelated channel on Flow from remote: " + channel));
        }

        flow.invoke(session, payload, channel, context);
    }

    @Override
    public void handleTransfer(Transfer transfer, ProtonBuffer payload, int channel, ProtonEngine context) {
        final ProtonSession session = remoteSessions.get(channel);
        if (session == null) {
            engine.engineFailed(new ProtocolViolationException("Received uncorrelated channel on Transfer from remote: " + channel));
        }

        transfer.invoke(session, payload, channel, context);
    }

    @Override
    public void handleDisposition(Disposition disposition, ProtonBuffer payload, int channel, ProtonEngine context) {
        final ProtonSession session = remoteSessions.get(channel);
        if (session == null) {
            engine.engineFailed(new ProtocolViolationException("Received uncorrelated channel on Disposition from remote: " + channel));
        }

        disposition.invoke(session, payload, channel, context);
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

    @Override
    public Connection receiverOpenEventHandler(EventHandler<Receiver> remoteReceiverOpenEventHandler) {
        this.remoteReceiverOpenEventHandler = remoteReceiverOpenEventHandler;
        return this;
    }

    //----- Internal implementation

    private void checkNotOpened(String errorMessage) {
        if (localState.ordinal() > ConnectionState.IDLE.ordinal()) {
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
                }

                if (getLocalState() == ConnectionState.CLOSED && !localCloseSent) {
                    localCloseSent = true;
                    engine.pipeline().fireWrite(new Close().setError(getLocalCondition()), 0, null, null);
                }
            }
        } else {
            headerSent = true;
            engine.pipeline().fireWrite(AMQPHeader.getAMQPHeader());
        }
    }

    private int findFreeLocalChannel() {
        for (int i = 0; i < CHANNEL_MAX; ++i) {
            if (!localSessions.containsKey(i)) {
                return i;
            }
        }

        throw new IllegalStateException("no local handle available for allocation");
    }

    private void freeLocalChannel(int localChannel) {
        if (localChannel > CHANNEL_MAX) {
            throw new IllegalArgumentException("Specified local channel is out of range: " + localChannel);
        }

        localSessions.remove(localChannel);
    }
}
