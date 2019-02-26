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
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.LinkState;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;
import org.apache.qpid.proton4j.engine.SessionState;

/**
 * Proton API for a Session endpoint
 */
public class ProtonSession implements Session, Performative.PerformativeHandler<ProtonEngine> {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonSession.class);

    private static final int HANDLE_MAX = 65535;  // TODO Honor handle max
    private static final int LINK_ARRAY_CHUNK_SIZE = 16;

    private final Begin localBegin = new Begin();
    private Begin remoteBegin;

    private int localChannel;

    private long nextIncomingId;
    private long nextOutgoingId = 1;
    private long incomingWindow;
    private long outgoingWindow;

    private final Map<String, ProtonSender> senderByNameMap = new HashMap<>();
    private final Map<String, ProtonReceiver> receiverByNameMap = new HashMap<>();

    // TODO - Space efficient primitive storage
    private ProtonLink<?>[] localLinks = new ProtonLink<?>[HANDLE_MAX];
    private ProtonLink<?>[] remoteLinks = new ProtonLink<?>[HANDLE_MAX];

    private final ProtonConnection connection;

    private EventHandler<AsyncEvent<Session>> remoteOpenHandler = (result) -> {
        LOG.trace("Remote session open arrived at default handler.");
    };
    private EventHandler<AsyncEvent<Session>> remoteCloseHandler = (result) -> {
        LOG.trace("Remote session close arrived at default handler.");
    };

    // No default for these handlers, Connection will process these if not set here.
    private EventHandler<Sender> remoteSenderOpenHandler = null;
    private EventHandler<Receiver> remoteReceiverOpenHandler = null;

    private Object context;
    private Map<String, Object> contextEntries = new HashMap<>();

    private SessionState localState = SessionState.IDLE;
    private SessionState remoteState = SessionState.IDLE;

    private ErrorCondition localError = new ErrorCondition();
    private ErrorCondition remoteError = new ErrorCondition();

    private boolean localBeginSent;
    private boolean localEndSent;

    public ProtonSession(ProtonConnection connection, int localChannel) {
        this.connection = connection;
        this.localChannel = localChannel;

        // Set initial state for Begin
        localBegin.setNextOutgoingId(nextOutgoingId);
        localBegin.setIncomingWindow(incomingWindow);
        localBegin.setOutgoingWindow(outgoingWindow);
    }

    @Override
    public Connection getConnection() {
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
    public SessionState getLocalState() {
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
    public SessionState getRemoteState() {
        return remoteState;
    }

    @Override
    public ErrorCondition getRemoteCondition() {
        return remoteError;
    }

    private void setRemoteCondition(ErrorCondition condition) {
        if (condition != null) {
            localError = condition.copy();
        } else {
            localError.clear();
        }
    }

    @Override
    public void open() {
        if (getLocalState() == SessionState.IDLE) {
            localState = SessionState.ACTIVE;
            connection.getEngine().pipeline().fireWrite(localBegin, localChannel, null, null);
        }
    }

    @Override
    public void close() {
        if (getLocalState() == SessionState.ACTIVE) {
            localState = SessionState.CLOSED;
            connection.getEngine().pipeline().fireWrite(new End().setError(getLocalCondition()), localChannel, null, null);
        }
    }

    @Override
    public void setProperties(Map<Symbol, Object> properties) {
        checkNotOpened("Cannot set Properties on already opened Session");

        if (properties != null) {
            localBegin.setProperties(new LinkedHashMap<>(properties));
        } else {
            localBegin.setProperties(properties);
        }
    }

    @Override
    public Map<Symbol, Object> getProperties() {
        if (localBegin.getProperties() != null) {
            return Collections.unmodifiableMap(localBegin.getProperties());
        }

        return null;
    }

    @Override
    public void setOfferedCapabilities(Symbol[] capabilities) {
        checkNotOpened("Cannot set Offered Capabilities on already opened Session");

        if (capabilities != null) {
            localBegin.setOfferedCapabilities(Arrays.copyOf(capabilities, capabilities.length));
        } else {
            localBegin.setOfferedCapabilities(capabilities);
        }
    }

    @Override
    public Symbol[] getOfferedCapabilities() {
        if (localBegin.getOfferedCapabilities() != null) {
            return Arrays.copyOf(localBegin.getOfferedCapabilities(), localBegin.getOfferedCapabilities().length);
        }

        return null;
    }

    @Override
    public void setDesiredCapabilities(Symbol[] capabilities) {
        checkNotOpened("Cannot set Desired Capabilities on already opened Session");

        if (capabilities != null) {
            localBegin.setDesiredCapabilities(Arrays.copyOf(capabilities, capabilities.length));
        } else {
            localBegin.setDesiredCapabilities(capabilities);
        }
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
        this.remoteSenderOpenHandler = remoteSenderOpenEventHandler;
        return this;
    }

    @Override
    public ProtonSession receiverOpenEventHandler(EventHandler<Receiver> remoteReceiverOpenEventHandler) {
        this.remoteReceiverOpenHandler = remoteReceiverOpenEventHandler;
        return this;
    }

    //----- Handle incoming performatives

    @Override
    public void handleBegin(Begin begin, ProtonBuffer payload, int channel, ProtonEngine context) {
        remoteBegin = begin;
        localBegin.setRemoteChannel(channel);
        remoteState = SessionState.ACTIVE;
        nextIncomingId = begin.getNextOutgoingId();

        if (getLocalState() == SessionState.ACTIVE) {
            remoteOpenHandler.handle(result(this, null));
        }
    }

    @Override
    public void handleEnd(End end, ProtonBuffer payload, int channel, ProtonEngine context) {
        // TODO - Fully implement handling for remote End

        setRemoteCondition(end.getError());
        remoteState = SessionState.CLOSED;

        remoteCloseHandler.handle(result(this, getRemoteCondition()));
    }

    @Override
    public void handleAttach(Attach attach, ProtonBuffer payload, int channel, ProtonEngine context) {
        if (validateHandleMaxCompliance(attach)) {
            // TODO - Space efficient primitive data structure
            if (remoteLinks.length > attach.getHandle() || remoteLinks[(int) attach.getHandle()] != null) {
                // TODO fail because link already in use.
                return;
            }

            ProtonLink<?> localLink = findMatchingPendingLinkOpen(attach);
            if (localLink == null) {
                localLink = (attach.getRole() == Role.RECEIVER) ? sender(attach.getName()) : receiver(attach.getName());
            }

            storeRemoteLink(localLink, (int) attach.getHandle());  // TODO Loss of handle fidelity

            attach.invoke(localLink, payload, channel, context);
        }
    }

    @Override
    public void handleDetach(Detach detach, ProtonBuffer payload, int channel, ProtonEngine context) {

    }

    @Override
    public void handleFlow(Flow flow, ProtonBuffer payload, int channel, ProtonEngine context) {

    }

    @Override
    public void handleTransfer(Transfer transfer, ProtonBuffer payload, int channel, ProtonEngine context) {

    }

    @Override
    public void handleDisposition(Disposition disposition, ProtonBuffer payload, int channel, ProtonEngine context) {

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

    int findFreeLocalHandle() {
        // We could eventually replace this with some more space efficient data structure like
        // a sparse array or possibly pull in a primitive map implementation.

        for (int i = 0; i < localLinks.length; ++i) {
            if (localLinks[i] == null) {
                return i;
            }
        }

        // TODO - Handle Max processing

        // resize to accommodate more links, new handle will be old length
        int handle = localLinks.length;
        localLinks = Arrays.copyOf(localLinks, localLinks.length + LINK_ARRAY_CHUNK_SIZE);
        return handle;
    }

    void freeLocalHandle(long localHandle) {
        if (localHandle > localLinks.length) {
            throw new IllegalArgumentException("Specified local handle is out of range: " + localHandle);
        }

        localLinks[localChannel] = null;
    }

    private void storeRemoteLink(ProtonLink<?> link, int remoteHandle) {
        // We could eventually replace this with some more space efficient data structure like
        // a sparse array or possibly pull in a primitive map implementation.

        if (remoteLinks.length <= remoteHandle) {
            // resize to accommodate more sessions, new channel will be old length
            remoteLinks = Arrays.copyOf(remoteLinks, remoteLinks.length + LINK_ARRAY_CHUNK_SIZE);
        }

        remoteLinks[remoteHandle] = link;
    }
}
