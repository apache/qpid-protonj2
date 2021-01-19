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
package org.apache.qpid.protonj2.engine.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.ConnectionState;
import org.apache.qpid.protonj2.engine.EventHandler;
import org.apache.qpid.protonj2.engine.Link;
import org.apache.qpid.protonj2.engine.LinkState;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.engine.SessionState;
import org.apache.qpid.protonj2.engine.TransactionController;
import org.apache.qpid.protonj2.engine.TransactionManager;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.engine.exceptions.EngineStateException;
import org.apache.qpid.protonj2.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.protonj2.engine.util.SplayMap;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.Attach;
import org.apache.qpid.protonj2.types.transport.Begin;
import org.apache.qpid.protonj2.types.transport.ConnectionError;
import org.apache.qpid.protonj2.types.transport.Detach;
import org.apache.qpid.protonj2.types.transport.Disposition;
import org.apache.qpid.protonj2.types.transport.End;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.apache.qpid.protonj2.types.transport.Flow;
import org.apache.qpid.protonj2.types.transport.Role;
import org.apache.qpid.protonj2.types.transport.SessionError;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Proton API for Session type.
 */
public class ProtonSession extends ProtonEndpoint<Session> implements Session {

    private final Begin localBegin = new Begin();
    private Begin remoteBegin;

    private int localChannel;

    private final ProtonSessionOutgoingWindow outgoingWindow;
    private final ProtonSessionIncomingWindow incomingWindow;

    private final Map<String, ProtonSender> senderByNameMap = new HashMap<>();
    private final Map<String, ProtonReceiver> receiverByNameMap = new HashMap<>();

    private final SplayMap<ProtonLink<?>> localLinks = new SplayMap<>();
    private final SplayMap<ProtonLink<?>> remoteLinks = new SplayMap<>();

    private final Flow cachedFlow = new Flow();

    private final ProtonConnection connection;

    private SessionState localState = SessionState.IDLE;
    private SessionState remoteState = SessionState.IDLE;

    private boolean localBeginSent;
    private boolean localEndSent;

    // No default for these handlers, Connection will process these if not set here.
    private EventHandler<Sender> remoteSenderOpenEventHandler;
    private EventHandler<Receiver> remoteReceiverOpenEventHandler;
    private EventHandler<TransactionManager> remoteTxnManagerOpenEventHandler;

    public ProtonSession(ProtonConnection connection, int localChannel) {
        super(connection.getEngine());

        this.connection = connection;
        this.localChannel = localChannel;

        this.outgoingWindow = new ProtonSessionOutgoingWindow(this);
        this.incomingWindow = new ProtonSessionIncomingWindow(this);
    }

    @Override
    ProtonSession self() {
        return this;
    }

    @Override
    public ProtonConnection getConnection() {
        return connection;
    }

    @Override
    public ProtonConnection getParent() {
        return connection;
    }

    public int getLocalChannel() {
        return localChannel;
    }

    public int getRemoteChannel() {
        return remoteBegin != null ? remoteBegin.getRemoteChannel() : -1;
    }

    @Override
    public SessionState getState() {
        return localState;
    }

    @Override
    public SessionState getRemoteState() {
        return remoteState;
    }

    @Override
    public ProtonSession open() throws IllegalStateException, EngineStateException {
        if (getState() == SessionState.IDLE) {
            checkConnectionClosed();
            getEngine().checkShutdownOrFailed("Cannot open a session when Engine is shutdown or failed.");

            localState = SessionState.ACTIVE;
            incomingWindow.configureOutbound(localBegin);
            outgoingWindow.configureOutbound(localBegin);
            try {
                trySyncLocalStateWithRemote();
            } finally {
                fireLocalOpen();
            }
        }

        return this;
    }

    @Override
    public ProtonSession close() throws EngineFailedException {
        if (getState() == SessionState.ACTIVE) {
            localState = SessionState.CLOSED;
            try {
                engine.checkFailed("Session close called but engine is in a failed state.");
                trySyncLocalStateWithRemote();
            } finally {
                allLinks().forEach(link -> link.handleSessionLocallyClosed(this));
                fireLocalClose();
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
    public int getRemainingIncomingCapacity() {
        return incomingWindow.getRemainingIncomingCapacity();
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

    @Override
    public Set<Link<?>> links() {
        return Collections.unmodifiableSet(allLinks());
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
    public boolean isRemotelyOpen() {
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

    @Override
    public TransactionController coordinator(String name) throws IllegalStateException {
        checkSessionClosed("Cannot create new TransactionController from closed Session");

        ProtonSender sender = senderByNameMap.get(name);

        if (sender == null) {
            sender = new ProtonSender(this, name);
            senderByNameMap.put(name, sender);
        }

        return new ProtonTransactionController(sender);
    }

    //----- Event handler registration for this Session

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
    public ProtonSession transactionManagerOpenHandler(EventHandler<TransactionManager> remoteTxnManagerOpenEventHandler) {
        this.remoteTxnManagerOpenEventHandler = remoteTxnManagerOpenEventHandler;
        return this;
    }

    EventHandler<TransactionManager> transactionManagerOpenHandler() {
        return remoteTxnManagerOpenEventHandler;
    }

    //----- Respond to Connection and Engine state changes

    void handleConnectionLocallyClosed(ProtonConnection protonConnection) {
        allLinks().forEach(link -> link.handleConnectionLocallyClosed(connection));
    }

    void handleConnectionRemotelyClosed(ProtonConnection protonConnection) {
        allLinks().forEach(link -> link.handleConnectionRemotelyClosed(connection));
    }

    void handleEngineShutdown(ProtonEngine protonEngine) {
        try {
            fireEngineShutdown();
        } catch (Throwable ingore) {}

        allLinks().forEach(link -> link.handleEngineShutdown(protonEngine));
    }

    //----- Handle incoming performatives

    void remoteBegin(Begin begin, int channel) {
        remoteBegin = begin;
        localBegin.setRemoteChannel(channel);
        remoteState = SessionState.ACTIVE;
        incomingWindow.handleBegin(begin);
        outgoingWindow.handleBegin(begin);

        if (isLocallyOpen()) {
            fireRemoteOpen();
        }
    }

    void remoteEnd(End end, int channel) {
        allLinks().forEach(link -> link.handleSessionRemotelyClosed(this));

        setRemoteCondition(end.getError());
        remoteState = SessionState.CLOSED;

        fireRemoteClose();
    }

    void remoteAttach(Attach attach, int channel) {
        if (validateHandleMaxCompliance(attach)) {
            if (remoteLinks.containsKey((int) attach.getHandle())) {
                setCondition(new ErrorCondition(SessionError.HANDLE_IN_USE, "Attach received with handle that is already in use"));
                close();
                return;
            }

            //TODO: nicer handling of the error
            if (!attach.hasInitialDeliveryCount() && attach.getRole() == Role.SENDER) {
                throw new ProtocolViolationException("Sending peer attach had no initial delivery count");
            }

            ProtonLink<?> link = findMatchingPendingLinkOpen(attach);
            if (link == null) {
                link = attach.getRole() == Role.RECEIVER ? sender(attach.getName()) : receiver(attach.getName());
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
            return;
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
        } else if (!link.isRemotelyOpen()) {
            getEngine().engineFailed(new ProtocolViolationException("Received Transfer for detached Receiver: " + link));
        } else {
            incomingWindow.handleTransfer(link, transfer, payload);
        }
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
        cachedFlow.reset();
        cachedFlow.setNextIncomingId(getIncomingWindow().getNextIncomingId());
        cachedFlow.setNextOutgoingId(getOutgoingWindow().getNextOutgoingId());
        cachedFlow.setIncomingWindow(getIncomingWindow().getIncomingWindow());
        cachedFlow.setOutgoingWindow(getOutgoingWindow().getOutgoingWindow());

        if (link != null) {
            link.decorateOutgoingFlow(cachedFlow);
        }

        getEngine().fireWrite(cachedFlow, localChannel, null, null);
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

    void trySyncLocalStateWithRemote() {
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

    private void checkIfBeginShouldBeSent() {
        if (!wasLocalBeginSent()) {
            if (connection.isLocallyOpen() && connection.wasLocalOpenSent()) {
                fireSessionBegin();
            }
        }
    }

    private void checkIfEndShouldBeSent() {
        if (!wasLocalEndSent()) {
            if (connection.isLocallyOpen() && connection.wasLocalOpenSent() && !engine.isShutdown()) {
                fireSessionEnd();
            }
        }
    }

    private void fireSessionBegin() {
        connection.getEngine().fireWrite(localBegin, localChannel, null, null);
        localBeginSent = true;
        allLinks().forEach(link -> link.trySyncLocalStateWithRemote());
    }

    private void fireSessionEnd() {
        connection.getEngine().fireWrite(new End().setError(getCondition()), localChannel, null, null);
        localEndSent = true;
        connection.freeLocalChannel(localChannel);
    }

    long findFreeLocalHandle(ProtonLink<?> link) {
        for (long i = 0; i < ProtonConstants.HANDLE_MAX; ++i) {
            if (!localLinks.containsKey((int) i)) {
                localLinks.put((int) i, link);
                return i;
            }
        }

        throw new IllegalStateException("no local handle available for allocation");
    }

    @SuppressWarnings("unchecked")
    private Set<ProtonLink<?>> allLinks() {
        final Set<ProtonLink<?>> result;

        if (senderByNameMap.isEmpty() && receiverByNameMap.isEmpty()) {
            return Collections.EMPTY_SET;
        } else {
            result = new HashSet<>(senderByNameMap.size());

            result.addAll(senderByNameMap.values());
            result.addAll(receiverByNameMap.values());
        }

        return result;
    }

    private void freeLocalHandle(long localHandle) {
        if (localHandle > ProtonConstants.HANDLE_MAX) {
            throw new IllegalArgumentException("Specified local handle is out of range: " + localHandle);
        }

        localLinks.remove((int) localHandle);
    }
}
