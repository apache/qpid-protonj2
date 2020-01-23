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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.ConnectionError;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.End;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;
import org.apache.qpid.proton4j.engine.ConnectionState;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.Link;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;
import org.apache.qpid.proton4j.engine.SessionState;
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.apache.qpid.proton4j.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.proton4j.engine.util.SplayMap;

/**
 * Proton API for Session type.
 */
public class ProtonSession implements Session {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonSession.class);

    private final Begin localBegin = new Begin();
    private Begin remoteBegin;

    private int localChannel;

    private final ProtonSessionOutgoingWindow outgoingWindow;
    private final ProtonSessionIncomingWindow incomingWindow;

    private final Map<String, ProtonSender> senderByNameMap = new HashMap<>();
    private final Map<String, ProtonReceiver> receiverByNameMap = new HashMap<>();

    private SplayMap<ProtonLink<?>> localLinks = new SplayMap<>();
    private SplayMap<ProtonLink<?>> remoteLinks = new SplayMap<>();

    private final ProtonConnection connection;
    private final ProtonEngine engine;
    private final ProtonContext context = new ProtonContext();

    private SessionState localState = SessionState.IDLE;
    private SessionState remoteState = SessionState.IDLE;

    private ErrorCondition localError;
    private ErrorCondition remoteError;

    private boolean localBeginSent;
    private boolean localEndSent;

    private EventHandler<Session> remoteOpenHandler = (result) -> {
        LOG.trace("Remote session open arrived at default handler.");
    };
    private EventHandler<Session> remoteCloseHandler = (result) -> {
        LOG.trace("Remote session close arrived at default handler.");
    };
    private EventHandler<Session> localOpenHandler = (result) -> {
        LOG.trace("Session locally opened.");
    };
    private EventHandler<Session> localCloseHandler = (result) -> {
        LOG.trace("Session locally closed.");
    };
    private EventHandler<Engine> engineShutdownHandler = (result) -> {
        LOG.trace("The underlying engine for this Session has been explicitly shutdown.");
    };

    // No default for these handlers, Connection will process these if not set here.
    private EventHandler<Sender> remoteSenderOpenEventHandler = null;
    private EventHandler<Receiver> remoteReceiverOpenEventHandler = null;

    public ProtonSession(ProtonConnection connection, int localChannel) {
        this.connection = connection;
        this.engine = connection.getEngine();
        this.localChannel = localChannel;

        this.outgoingWindow = new ProtonSessionOutgoingWindow(this);
        this.incomingWindow = new ProtonSessionIncomingWindow(this);
    }

    @Override
    public ProtonConnection getConnection() {
        return connection;
    }

    @Override
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
    public SessionState getState() {
        return localState;
    }

    @Override
    public ErrorCondition getCondition() {
        return localError;
    }

    @Override
    public ProtonSession setCondition(ErrorCondition condition) {
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
    public ProtonSession open() throws IllegalStateException, EngineStateException {
        checkConnectionClosed();
        getEngine().checkShutdownOrFailed("Cannot open a session when Engine is shutdown or failed.");

        if (getState() == SessionState.IDLE) {
            localState = SessionState.ACTIVE;
            incomingWindow.configureOutbound(localBegin);
            outgoingWindow.configureOutbound(localBegin);
            try {
                syncLocalStateWithRemote();
            } finally {
                if (localOpenHandler != null) {
                    localOpenHandler.handle(this);
                }
            }
        }

        return this;
    }

    @Override
    public ProtonSession close() throws EngineFailedException {
        if (getState() == SessionState.ACTIVE) {
            engine.checkFailed("Cannot close the session when engine is in the failed state");
            localState = SessionState.CLOSED;
            try {
                syncLocalStateWithRemote();
            } finally {
                if (localCloseHandler != null) {
                    localCloseHandler.handle(this);
                }
            }
        }

        return this;
    }

    //----- View and configure this end of the session endpoint

    @Override
    public boolean isLocallyOpen() {
        return getState() == SessionState.ACTIVE;
    }

    @Override
    public boolean isLocallyClosed() {
        return getState() == SessionState.CLOSED;
    }

    @Override
    public Session setIncomingCapacity(int incomingCapacity) {
        incomingWindow.setIncomingCapaity(incomingCapacity);
        return this;
    }

    @Override
    public int getIncomingCapacity() {
        return incomingWindow.getIncomingCapacity();
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
    public ProtonSession setOfferedCapabilities(Symbol... capabilities) {
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
    public ProtonSession setDesiredCapabilities(Symbol... capabilities) {
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

    @SuppressWarnings("unchecked")
    @Override
    public Set<Link<?>> links() {
        final Set<Link<?>> result;

        if (senderByNameMap.isEmpty() && receiverByNameMap.isEmpty()) {
            result = Collections.EMPTY_SET;
        } else {
            result = new HashSet<>(senderByNameMap.size() + receiverByNameMap.size());
            result.addAll(senderByNameMap.values());
            result.addAll(receiverByNameMap.values());
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Sender> senders() {
        final Set<Sender> result;

        if (senderByNameMap.isEmpty()) {
            result = Collections.EMPTY_SET;
        } else {
            result = new HashSet<>(senderByNameMap.values());
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Receiver> receivers() {
        final Set<Receiver> result;

        if (receiverByNameMap.isEmpty()) {
            result = Collections.EMPTY_SET;
        } else {
            result = new HashSet<>(receiverByNameMap.values());
        }

        return result;
    }

    //----- View of the remote end of this endpoint

    @Override
    public boolean isRemotelyOpened() {
        return getRemoteState() == SessionState.ACTIVE;
    }

    @Override
    public boolean isRemotelyClosed() {
        return getRemoteState() == SessionState.CLOSED;
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

    //----- Session factory methods for Sender and Receiver

    @Override
    public ProtonSender sender(String name) {
        checkSessionClosed("Cannot create new Sender from closed Session");

        ProtonSender sender = senderByNameMap.get(name);

        if (sender == null) {
            sender = new ProtonSender(this, name);
            senderByNameMap.put(name, sender);
        }

        return sender;
    }

    @Override
    public ProtonReceiver receiver(String name) {
        checkSessionClosed("Cannot create new Receiver from closed Session");

        ProtonReceiver receiver = receiverByNameMap.get(name);

        if (receiver == null) {
            receiver = new ProtonReceiver(this, name);
            receiverByNameMap.put(name, receiver);
        }

        return receiver;
    }

    //----- Event handler registration for this Session

    @Override
    public ProtonSession openHandler(EventHandler<Session> remoteOpenEventHandler) {
        this.remoteOpenHandler = remoteOpenEventHandler;
        return this;
    }

    @Override
    public ProtonSession closeHandler(EventHandler<Session> remoteCloseEventHandler) {
        this.remoteCloseHandler = remoteCloseEventHandler;
        return this;
    }

    @Override
    public ProtonSession localOpenHandler(EventHandler<Session> localOpenEventHandler) {
        this.localOpenHandler = localOpenEventHandler;
        return this;
    }

    @Override
    public ProtonSession localCloseHandler(EventHandler<Session> localCloseEventHandler) {
        this.localCloseHandler = localCloseEventHandler;
        return this;
    }

    @Override
    public ProtonSession senderOpenHandler(EventHandler<Sender> remoteSenderOpenEventHandler) {
        this.remoteSenderOpenEventHandler = remoteSenderOpenEventHandler;
        return this;
    }

    EventHandler<Sender> senderOpenEventHandler() {
        return remoteSenderOpenEventHandler;
    }

    @Override
    public ProtonSession receiverOpenHandler(EventHandler<Receiver> remoteReceiverOpenEventHandler) {
        this.remoteReceiverOpenEventHandler = remoteReceiverOpenEventHandler;
        return this;
    }

    EventHandler<Receiver> receiverOpenEventHandler() {
        return remoteReceiverOpenEventHandler;
    }

    @Override
    public ProtonSession engineShutdownHandler(EventHandler<Engine> engineShutdownEventHandler) {
        this.engineShutdownHandler = engineShutdownEventHandler;
        return this;
    }

    EventHandler<Engine> engineShutdownHandler() {
        return engineShutdownHandler;
    }

    //----- Respond to local Connection changes

    void handleConnectionStateChanged(ProtonConnection connection) {
        switch (connection.getState()) {
            case IDLE:
                return;
            case ACTIVE:
                syncLocalStateWithRemote();
                return;
            case CLOSED:
                processParentConnectionLocallyClosed();
                return;
        }
    }

    void handleEngineShutdown(ProtonEngine protonEngine) {
        Set<ProtonLink<?>> links = new HashSet<>();

        links.addAll(localLinks.values());
        links.addAll(remoteLinks.values());

        links.forEach(link -> {
            try {
                link.handleEngineShutdown(protonEngine);
            } catch (Throwable ignore) {}
        });

        try {
            engineShutdownHandler.handle(protonEngine);
        } catch (Throwable ingore) {}
    }

    //----- Handle incoming performatives

    void remoteBegin(Begin begin, int channel) {
        remoteBegin = begin;
        localBegin.setRemoteChannel(channel);
        remoteState = SessionState.ACTIVE;
        incomingWindow.handleBegin(begin);
        outgoingWindow.handleBegin(begin);

        if (isLocallyOpen() && remoteOpenHandler != null) {
            remoteOpenHandler.handle(this);
        }
    }

    void remoteEnd(End end, int channel) {
        // TODO - Fully implement handling for remote End

        setRemoteCondition(end.getError());
        remoteState = SessionState.CLOSED;

        if (remoteCloseHandler != null) {
            remoteCloseHandler.handle(this);
        }
    }

    void remoteAttach(Attach attach, int channel) {
        if (validateHandleMaxCompliance(attach)) {
            if (remoteLinks.containsKey((int) attach.getHandle())) {
                // TODO fail because link already in use.
                return;
            }

            //TODO: nicer handling of the error
            if (!attach.hasInitialDeliveryCount() && attach.getRole() == Role.SENDER) {
                throw new IllegalArgumentException("Sending peer attach had no initial delivery count");
            }

            ProtonLink<?> link = findMatchingPendingLinkOpen(attach);
            if (link == null) {
                link = (attach.getRole() == Role.RECEIVER) ? sender(attach.getName()) : receiver(attach.getName());
            }

            remoteLinks.put((int) attach.getHandle(), link);

            link.remoteAttach(attach);
        }
    }

    void remoteDetach(Detach detach, int channel) {
        final ProtonLink<?> link = remoteLinks.remove((int) detach.getHandle());
        if (link == null) {
            getEngine().engineFailed(new ProtocolViolationException(
                "Received uncorrelated handle on Detach from remote: " + channel));
        }

        // Ensure that tracked links get cleared at some point as we don't currently have the concept
        // of link free APIs to put this onto the user to manage.
        if (link.isLocallyClosed() || link.isLocallyDetached()) {
            if (link.isReceiver()) {
                receiverByNameMap.remove(link.getName());
            } else {
                senderByNameMap.remove(link.getName());
            }
         }

        link.remoteDetach(detach);
    }

    void remoteFlow(Flow flow, int channel) {
        // Session level flow processing.
        incomingWindow.handleFlow(flow);
        outgoingWindow.handleFlow(flow);

        final ProtonLink<?> link;

        if (flow.hasHandle()) {
            link = remoteLinks.get((int) flow.getHandle());
            if (link == null) {
                getEngine().engineFailed(new ProtocolViolationException(
                    "Received uncorrelated handle on Flow from remote: " + channel));
                return;
            }

            link.remoteFlow(flow);
        } else {
            link = null;
        }

        //TODO: perhaps make this optional 'auto-echo'? Otherwise listeners above might not have had time to
        //      perform desired work before below occurs.
        if (flow.getEcho()) {
            writeFlow(link);
        }
    }

    void remoteTransfer(Transfer transfer, ProtonBuffer payload, int channel) {
        final ProtonLink<?> link = remoteLinks.get((int) transfer.getHandle());
        if (link == null) {
            getEngine().engineFailed(new ProtocolViolationException(
                "Received uncorrelated handle on Transfer from remote: " + channel));
        }

        if (!link.isRemotelyOpened()) {
            getEngine().engineFailed(new ProtocolViolationException("Received Transfer for detached Receiver: " + link));
        }

        incomingWindow.handleTransfer(link, transfer, payload);
    }

    void remoteDispsotion(Disposition disposition, int channel) {
        if (disposition.getRole() == Role.RECEIVER) {
            outgoingWindow.handleDisposition(disposition);
        } else {
            incomingWindow.handleDisposition(disposition);
        }
    }

    //----- Internal implementation

    ProtonSessionOutgoingWindow getOutgoingWindow() {
        return outgoingWindow;
    }

    ProtonSessionIncomingWindow getIncomingWindow() {
        return incomingWindow;
    }

    boolean wasLocalBeginSent() {
        return localBeginSent;
    }

    boolean wasLocalEndSent() {
        return localEndSent;
    }

    void freeLink(ProtonLink<?> linkToFree) {
        freeLocalHandle(linkToFree.getHandle());

        if (linkToFree.isRemotelyClosed() || linkToFree.isRemotelyDetached()) {
            if (linkToFree.isReceiver()) {
                receiverByNameMap.remove(linkToFree.getName());
            } else {
                senderByNameMap.remove(linkToFree.getName());
            }
        }
    }

    void writeFlow(ProtonLink<?> link) {
        final Flow flow = new Flow();

        flow.setNextIncomingId(getIncomingWindow().getNextIncomingId());
        flow.setNextOutgoingId(getOutgoingWindow().getNextOutgoingId());
        flow.setIncomingWindow(getIncomingWindow().getIncomingWindow());
        flow.setOutgoingWindow(getOutgoingWindow().getOutgoingWindow());

        if (link != null) {
            flow.setLinkCredit(link.linkState().getCredit());
            flow.setHandle(link.getHandle());
            if (link.isDeliveryCountInitialised()) {
                //TODO: type mismatch, will fail on deliveryCount wrap
                flow.setDeliveryCount(link.linkState().getDeliveryCount());
            }
            flow.setDrain(link.isDrain());
        }

        getEngine().fireWrite(flow, localChannel, null, null);
    }

    private void checkNotOpened(String errorMessage) {
        if (localState.ordinal() > SessionState.IDLE.ordinal()) {
            throw new IllegalStateException(errorMessage);
        }
    }

    private void checkConnectionClosed() {
        if (connection.getState() == ConnectionState.CLOSED || connection.getRemoteState() == ConnectionState.CLOSED) {
             throw new IllegalStateException("Cannot open a Session from a Connection that is already closed");
        }
    }

    private void checkSessionClosed(String errorMessage) {
        if (isLocallyClosed() || isRemotelyClosed()) {
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
            connection.setCondition(condition);
            connection.close();

            return false;
        }

        return true;
    }

    private void syncLocalStateWithRemote() {
        switch (getState()) {
            case IDLE:
                return;
            case ACTIVE:
                checkIfBeginShouldBeSent();
                break;
            case CLOSED:
                checkIfBeginShouldBeSent();
                checkIfEndShouldBeSent();
                break;
            default:
                throw new IllegalStateException("Session is in unknown state and cannot proceed");
        }
    }

    private void processParentConnectionLocallyClosed() {
        for (ProtonLink<?> link : localLinks.values()) {
            link.processParentConnectionLocallyClosed();
        }
    }

    private void checkIfBeginShouldBeSent() {
        if (!localBeginSent) {
            if (connection.isLocallyOpen() && connection.wasLocalOpenSent()) {
                fireSessionBegin();
            }
        }
    }

    private void checkIfEndShouldBeSent() {
        if (!localEndSent) {
            if (connection.isLocallyOpen() && connection.wasLocalOpenSent() && !engine.isShutdown()) {
                fireSessionEnd();
            }
        }
    }

    private void fireSessionBegin() {
        localBeginSent = true;
        connection.getEngine().fireWrite(localBegin, localChannel, null, null);
        localLinks.forEach(link -> link.handleSessionStateChanged(this));
    }

    private void fireSessionEnd() {
        localEndSent = true;
        connection.freeLocalChannel(localChannel);
        connection.getEngine().fireWrite(new End().setError(getCondition()), localChannel, null, null);
        localLinks.forEach(link -> link.handleSessionStateChanged(this));
    }

    long findFreeLocalHandle(ProtonLink<?> link) {
        for (int i = 0; i < ProtonConstants.HANDLE_MAX; ++i) {
            if (!localLinks.containsKey(i)) {
                localLinks.put(i, link);
                return i;
            }
        }

        throw new IllegalStateException("no local handle available for allocation");
    }

    private void freeLocalHandle(long localHandle) {
        if (localHandle > ProtonConstants.HANDLE_MAX) {
            throw new IllegalArgumentException("Specified local handle is out of range: " + localHandle);
        }

        localLinks.remove((int) localHandle);
    }
}
