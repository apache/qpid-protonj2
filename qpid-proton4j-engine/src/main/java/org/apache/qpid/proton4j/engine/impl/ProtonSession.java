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
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.ConnectionError;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.End;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;
import org.apache.qpid.proton4j.engine.AsyncEvent;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;
import org.apache.qpid.proton4j.engine.SessionState;
import org.apache.qpid.proton4j.engine.exceptions.ProtocolViolationException;

/**
 * Proton API for Session type.
 */
public class ProtonSession implements Session, Performative.PerformativeHandler<ProtonEngine> {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonSession.class);

    private final Begin localBegin = new Begin();
    private Begin remoteBegin;

    private int localChannel;

    @SuppressWarnings("unused")
    private long nextIncomingId;  // TODO
    private long nextOutgoingId = 1;
    private long incomingWindow;
    private long outgoingWindow;

    private final Map<String, ProtonSender> senderByNameMap = new HashMap<>();
    private final Map<String, ProtonReceiver> receiverByNameMap = new HashMap<>();

    // TODO - Space efficient primitive storage
    private Map<Long, ProtonLink<?>> localLinks = new HashMap<>();
    private Map<Long, ProtonLink<?>> remoteLinks = new HashMap<>();

    private final ProtonConnection connection;
    private final ProtonContext context = new ProtonContext();

    private SessionState localState = SessionState.IDLE;
    private SessionState remoteState = SessionState.IDLE;

    private ErrorCondition localError;
    private ErrorCondition remoteError;

    private EventHandler<AsyncEvent<Session>> remoteOpenHandler = (result) -> {
        LOG.trace("Remote session open arrived at default handler.");
    };
    private EventHandler<AsyncEvent<Session>> remoteCloseHandler = (result) -> {
        LOG.trace("Remote session close arrived at default handler.");
    };

    // No default for these handlers, Connection will process these if not set here.
    private EventHandler<Sender> remoteSenderOpenEventHandler = null;
    private EventHandler<Receiver> remoteReceiverOpenEventHandler = null;

    public ProtonSession(ProtonConnection connection, int localChannel) {
        this.connection = connection;
        this.localChannel = localChannel;

        // Set initial state for Begin
        localBegin.setNextOutgoingId(nextOutgoingId);
        localBegin.setIncomingWindow(incomingWindow);
        localBegin.setOutgoingWindow(outgoingWindow);
    }

    @Override
    public ProtonConnection getConnection() {
        return connection;
    }

    public ProtonEngine getEngine() {
        return connection.getEngine();
    }

    public int getLocalChannel() {
        return localChannel;
    }

    public int getRemoteChannel() {
        return remoteBegin != null ? remoteBegin.getRemoteChannel() : -1;
    }

    @Override
    public ProtonContext getContext() {
        return context;
    }

    @Override
    public SessionState getLocalState() {
        return localState;
    }

    @Override
    public ErrorCondition getLocalCondition() {
        return localError;
    }

    @Override
    public ProtonSession setLocalCondition(ErrorCondition condition) {
        localError = condition == null ? null : condition.copy();
        return this;
    }

    @Override
    public SessionState getRemoteState() {
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
    public ProtonSession open() {
        if (getLocalState() == SessionState.IDLE) {
            localState = SessionState.ACTIVE;
            connection.getEngine().pipeline().fireWrite(localBegin, localChannel, null, null);
        }

        return this;
    }

    @Override
    public ProtonSession close() {
        if (getLocalState() == SessionState.ACTIVE) {
            localState = SessionState.CLOSED;
            connection.freeLocalChannel(localChannel);
            connection.getEngine().pipeline().fireWrite(new End().setError(getLocalCondition()), localChannel, null, null);
        }

        return this;
    }

    @Override
    public ProtonSession setProperties(Map<Symbol, Object> properties) {
        checkNotOpened("Cannot set Properties on already opened Session");

        if (properties != null) {
            localBegin.setProperties(new LinkedHashMap<>(properties));
        } else {
            localBegin.setProperties(properties);
        }

        return this;
    }

    @Override
    public Map<Symbol, Object> getProperties() {
        if (localBegin.getProperties() != null) {
            return Collections.unmodifiableMap(localBegin.getProperties());
        }

        return null;
    }

    @Override
    public ProtonSession setOfferedCapabilities(Symbol[] capabilities) {
        checkNotOpened("Cannot set Offered Capabilities on already opened Session");

        if (capabilities != null) {
            localBegin.setOfferedCapabilities(Arrays.copyOf(capabilities, capabilities.length));
        } else {
            localBegin.setOfferedCapabilities(capabilities);
        }

        return this;
    }

    @Override
    public Symbol[] getOfferedCapabilities() {
        if (localBegin.getOfferedCapabilities() != null) {
            return Arrays.copyOf(localBegin.getOfferedCapabilities(), localBegin.getOfferedCapabilities().length);
        }

        return null;
    }

    @Override
    public ProtonSession setDesiredCapabilities(Symbol[] capabilities) {
        checkNotOpened("Cannot set Desired Capabilities on already opened Session");

        if (capabilities != null) {
            localBegin.setDesiredCapabilities(Arrays.copyOf(capabilities, capabilities.length));
        } else {
            localBegin.setDesiredCapabilities(capabilities);
        }

        return this;
    }

    @Override
    public Symbol[] getDesiredCapabilities() {
        if (localBegin.getDesiredCapabilities() != null) {
            return Arrays.copyOf(localBegin.getDesiredCapabilities(), localBegin.getDesiredCapabilities().length);
        }

        return null;
    }

    @Override
    public Symbol[] getRemoteOfferedCapabilities() {
        if (remoteBegin != null && remoteBegin.getOfferedCapabilities() != null) {
            return Arrays.copyOf(remoteBegin.getOfferedCapabilities(), remoteBegin.getOfferedCapabilities().length);
        }

        return null;
    }

    @Override
    public Symbol[] getRemoteDesiredCapabilities() {
        if (remoteBegin != null && remoteBegin.getDesiredCapabilities() != null) {
            return Arrays.copyOf(remoteBegin.getDesiredCapabilities(), remoteBegin.getDesiredCapabilities().length);
        }

        return null;
    }

    @Override
    public Map<Symbol, Object> getRemoteProperties() {
        if (remoteBegin != null && remoteBegin.getProperties() != null) {
            return Collections.unmodifiableMap(remoteBegin.getProperties());
        }

        return null;
    }

    //----- Session factory methods

    @Override
    public ProtonSender sender(String name) {
        if (senderByNameMap.containsKey(name)) {
            // TODO Something sane with link stealing
            throw new IllegalArgumentException("Sender with the given name already exists.");
        }

        ProtonSender sender = new ProtonSender(this, name);
        senderByNameMap.put(name, sender);

        return sender;
    }

    @Override
    public ProtonReceiver receiver(String name) {
        if (receiverByNameMap.containsKey(name)) {
            // TODO Something sane with link stealing
            throw new IllegalArgumentException("Receiver with the given name already exists.");
        }

        ProtonReceiver receiver = new ProtonReceiver(this, name);
        receiverByNameMap.put(name, receiver);

        return receiver;
    }

    //----- Event handler registration for this Session

    @Override
    public ProtonSession openHandler(EventHandler<AsyncEvent<Session>> remoteOpenEventHandler) {
        this.remoteOpenHandler = remoteOpenEventHandler;
        return this;
    }

    @Override
    public ProtonSession closeHandler(EventHandler<AsyncEvent<Session>> remoteCloseEventHandler) {
        this.remoteCloseHandler = remoteCloseEventHandler;
        return this;
    }

    @Override
    public ProtonSession senderOpenEventHandler(EventHandler<Sender> remoteSenderOpenEventHandler) {
        this.remoteSenderOpenEventHandler = remoteSenderOpenEventHandler;
        return this;
    }

    EventHandler<Sender> senderOpenEventHandler() {
        return remoteSenderOpenEventHandler;
    }

    @Override
    public ProtonSession receiverOpenEventHandler(EventHandler<Receiver> remoteReceiverOpenEventHandler) {
        this.remoteReceiverOpenEventHandler = remoteReceiverOpenEventHandler;
        return this;
    }

    EventHandler<Receiver> receiverOpenEventHandler() {
        return remoteReceiverOpenEventHandler;
    }

    //----- Handle incoming performatives

    @Override
    public void handleBegin(Begin begin, ProtonBuffer payload, int channel, ProtonEngine context) {
        remoteBegin = begin;
        localBegin.setRemoteChannel(channel);
        remoteState = SessionState.ACTIVE;
        nextIncomingId = begin.getNextOutgoingId();

        if (getLocalState() == SessionState.ACTIVE && remoteOpenHandler != null) {
            remoteOpenHandler.handle(result(this, null));
        }
    }

    @Override
    public void handleEnd(End end, ProtonBuffer payload, int channel, ProtonEngine context) {
        // TODO - Fully implement handling for remote End

        setRemoteCondition(end.getError());
        remoteState = SessionState.CLOSED;

        if (remoteCloseHandler != null) {
            remoteCloseHandler.handle(result(this, getRemoteCondition()));
        }
    }

    @Override
    public void handleAttach(Attach attach, ProtonBuffer payload, int channel, ProtonEngine context) {
        if (validateHandleMaxCompliance(attach)) {
            // TODO - Space efficient primitive data structure
            if (remoteLinks.containsKey(attach.getHandle())) {
                // TODO fail because link already in use.
                return;
            }

            ProtonLink<?> link = findMatchingPendingLinkOpen(attach);
            if (link == null) {
                link = (attach.getRole() == Role.RECEIVER) ? sender(attach.getName()) : receiver(attach.getName());
            }

            remoteLinks.put(attach.getHandle(), link);

            attach.invoke(link, payload, channel, context);
        }
    }

    @Override
    public void handleDetach(Detach detach, ProtonBuffer payload, int channel, ProtonEngine context) {
        final ProtonLink<?> link = remoteLinks.get(detach.getHandle());
        if (link == null) {
            getEngine().engineFailed(new ProtocolViolationException("Received uncorrelated handle on Detach from remote: " + channel));
        }

        detach.invoke(link, payload, channel, context);
    }

    @Override
    public void handleFlow(Flow flow, ProtonBuffer payload, int channel, ProtonEngine context) {
        final ProtonLink<?> link = remoteLinks.get(flow.getHandle());
        if (link == null) {
            getEngine().engineFailed(new ProtocolViolationException("Received uncorrelated handle on Flow from remote: " + channel));
        }

        flow.invoke(link, payload, channel, context);
    }

    @Override
    public void handleTransfer(Transfer transfer, ProtonBuffer payload, int channel, ProtonEngine context) {
        final ProtonLink<?> link = remoteLinks.get(transfer.getHandle());
        if (link == null) {
            getEngine().engineFailed(new ProtocolViolationException("Received uncorrelated handle on Transfer from remote: " + channel));
        }

        transfer.invoke(link, payload, channel, context);
    }

    @Override
    public void handleDisposition(Disposition disposition, ProtonBuffer payload, int channel, ProtonEngine context) {
        // TODO
    }

    //----- Internal implementation

    private void checkNotOpened(String errorMessage) {
        if (localState.ordinal() > SessionState.IDLE.ordinal()) {
            throw new IllegalStateException(errorMessage);
        }
    }

    private ProtonLink<?> findMatchingPendingLinkOpen(Attach remoteAttach) {
        for (ProtonLink<?> link : senderByNameMap.values()) {
            if (link.getName().equals(remoteAttach.getName()) &&
                link.getRemoteState() == LinkState.IDLE &&
                link.getRole() != remoteAttach.getRole()) {

                return link;
            }
        }

        for (ProtonLink<?> link : receiverByNameMap.values()) {
            if (link.getName().equals(remoteAttach.getName()) &&
                link.getRemoteState() == LinkState.IDLE &&
                link.getRole() != remoteAttach.getRole()) {

                return link;
            }
        }

        return null;
    }

    private boolean validateHandleMaxCompliance(Attach remoteAttach) {
        final long remoteHandle = remoteAttach.getHandle();
        if (localBegin.getHandleMax() < remoteHandle) {
            // The handle-max value is the highest handle value that can be used on the session. A peer MUST
            // NOT attempt to attach a link using a handle value outside the range that its partner can handle.
            // A peer that receives a handle outside the supported range MUST close the connection with the
            // framing-error error-code.
            ErrorCondition condition = new ErrorCondition(ConnectionError.FRAMING_ERROR, "Session handle-max exceeded");

            // TODO - Provide way to close the connection from this end in error.
            connection.setLocalCondition(condition);

            return false;
        }

        return true;
    }

    long findFreeLocalHandle() {
        for (long i = 0; i < ProtonConstants.HANDLE_MAX; ++i) {
            if (!localLinks.containsKey(i)) {
                return i;
            }
        }

        throw new IllegalStateException("no local handle available for allocation");
    }

    void freeLocalHandle(long localHandle) {
        if (localHandle > ProtonConstants.HANDLE_MAX) {
            throw new IllegalArgumentException("Specified local handle is out of range: " + localHandle);
        }

        localLinks.remove(localHandle);
    }
}
