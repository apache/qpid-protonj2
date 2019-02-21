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
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.End;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;
import org.apache.qpid.proton4j.engine.AsyncEvent;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.EndpointState;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.engine.Sender;
import org.apache.qpid.proton4j.engine.Session;

/**
 * Proton API for a Session endpoint
 */
public class ProtonSession extends ProtonEndpoint<Session> implements Session, Performative.PerformativeHandler<ProtonEngine> {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonSession.class);

    private final Begin localBegin = new Begin();
    private Begin remoteBegin;

    private int localChannel;

    private long nextIncomingId;
    private long nextOutgoingId = 1;
    private long incomingWindow;
    private long outgoingWindow;

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
    public Sender sender(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Receiver receiver(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    //----- Internal state methods

    @Override
    void initiateLocalOpen() {
        connection.getEngine().pipeline().fireWrite(localBegin, localChannel, null, null);
    }

    @Override
    void initiateLocalClose() {
        connection.getEngine().pipeline().fireWrite(new End().setError(getLocalCondition()), localChannel, null, null);
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
        remoteOpenWasReceived();
        nextIncomingId = begin.getNextOutgoingId();

        if (getLocalState() == EndpointState.ACTIVE) {
            remoteOpenHandler.handle(result(this, null));
        }
    }

    @Override
    public void handleEnd(End end, ProtonBuffer payload, int channel, ProtonEngine context) {
        // TODO - Fully implement handling for remote End

        setRemoteCondition(end.getError());
        remoteClosedWasReceived();

        remoteCloseHandler.handle(result(this, getRemoteCondition()));
    }

    @Override
    public void handleAttach(Attach attach, ProtonBuffer payload, int channel, ProtonEngine context) {

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

    @Override
    public void handleDetach(Detach detach, ProtonBuffer payload, int channel, ProtonEngine context) {

    }
}
