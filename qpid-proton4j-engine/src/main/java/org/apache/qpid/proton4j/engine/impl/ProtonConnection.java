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
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Close;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.End;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Open;
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
import org.apache.qpid.proton4j.engine.exceptions.ProtonException;

/**
 * Implements the proton4j Connection API
 */
public class ProtonConnection extends ProtonEndpoint<Connection> implements Connection, AMQPHeader.HeaderHandler<ProtonEngine>, Performative.PerformativeHandler<ProtonEngine> {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonConnection.class);

    private static final int SESSION_ARRAY_CHUNK_SIZE = 16;

    private final ProtonEngine engine;

    private final Open localOpen = new Open();
    private Open remoteOpen;

    private ProtonSession[] localSessions = new ProtonSession[SESSION_ARRAY_CHUNK_SIZE];

    private EventHandler<AsyncEvent<Connection>> remoteOpenHandler = (result) -> {
        LOG.trace("Remote open arrived at default handler.");
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
    public String getContainerId() {
        return localOpen.getContainerId();
    }

    @Override
    public void setContainerId(String containerId) {
        localOpen.setContainerId(containerId);
    }

    @Override
    public void setHostname(String hostname) {
        localOpen.setHostname(hostname);
    }

    @Override
    public String getHostname() {
        return localOpen.getHostname();
    }

    @Override
    public void setChannelMax(int channelMax) {
        localOpen.setChannelMax(UnsignedShort.valueOf((short) channelMax));
    }

    @Override
    public int getChannelMax() {
        return localOpen.getChannelMax().intValue();
    }

    @Override
    public void setIdleTimeout(int idleTimeout) {
        localOpen.setIdleTimeOut(UnsignedInteger.valueOf(idleTimeout));
    }

    @Override
    public int getIdleTimeout() {
        return localOpen.getIdleTimeOut().intValue();
    }

    @Override
    public void setOfferedCapabilities(Symbol[] capabilities) {
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

        return null;  // TODO Empty Array instead ?
    }

    @Override
    public Symbol[] getRemoteDesiredCapabilities() {
        if (remoteOpen != null && remoteOpen.getDesiredCapabilities() != null) {
            return Arrays.copyOf(remoteOpen.getDesiredCapabilities(), remoteOpen.getDesiredCapabilities().length);
        }

        return null;  // TODO Empty Array instead ?
    }

    @Override
    public Map<Symbol, Object> getRemoteProperties() {
        if (remoteOpen != null && remoteOpen.getProperties() != null) {
            return Collections.unmodifiableMap(remoteOpen.getProperties());
        }

        return null;  // TODO Empty Map instead ?
    }

    @Override
    public ProtonSession session() {
        // TODO Auto-generated method stub
        return null;
    }

    //----- Handle internal state changes

    @Override
    void initiateLocalOpen() {
        // TODO - Handler already having received a header
        engine.pipeline().fireWrite(AMQPHeader.getAMQPHeader());
    }

    @Override
    void initiateLocalClose() {
        // TODO Auto-generated method stub
    }

    int findFreeLocalChannel() {
        for (int i = 0; i < localSessions.length; ++i) {
            if (localSessions[i] == null) {
                return i;
            }
        }

        // resize to accommodate more sessions, new channel will be old length
        int channel = localSessions.length;
        localSessions = Arrays.copyOf(localSessions, localSessions.length + SESSION_ARRAY_CHUNK_SIZE);
        return channel;
    }

    void freeLocalChannel(int localChannel) {
        if (localChannel > localSessions.length) {
            throw new IllegalArgumentException("Specified local channel is out of range: " + localChannel);
        }
        localSessions[localChannel] = null;
    }

    //----- Handle performatives sent from the remote to this Connection

    @Override
    public void handleAMQPHeader(AMQPHeader header, ProtonEngine context) {
        // Once an incoming header arrives we can emit our open if locally opened and also send close if
        // that is what our state is already.
        if (getLocalState() != EndpointState.IDLE) {
            if (!wasLocalOpenSent()) {
                localCloseWasSent();
                context.pipeline().fireWrite(localOpen, (short) 0, null, null);
            }

            if (getLocalState().ordinal() > EndpointState.ACTIVE.ordinal() && !wasLocalCloseSent()) {
                localCloseWasSent();
                context.pipeline().fireWrite(new Close().setError(getLocalCondition()), (short) 0, null, null);
            }
        }
    }

    @Override
    public void handleSASLHeader(AMQPHeader header, ProtonEngine context) {
        context.engineFailed(new ProtonException("Receivded unexpected SASL Header"));
    }

    @Override
    public void handleOpen(Open open, ProtonBuffer payload, int channel, ProtonEngine context) {
        if (remoteOpen != null) {
            // TODO - Throw error indicating invalid state remote open already received.
        }

        remoteOpenWasReceived();
        remoteOpen = open;
        remoteOpenHandler.handle(result(this, null));
    }

    @Override
    public void handleBegin(Begin begin, ProtonBuffer payload, int channel, ProtonEngine context) {

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

    @Override
    public void handleEnd(End end, ProtonBuffer payload, int channel, ProtonEngine context) {

    }

    @Override
    public void handleClose(Close close, ProtonBuffer payload, int channel, ProtonEngine context) {

    }

    //----- API for event handling of Connection related remote events

    @Override
    public Connection openEventHandler(EventHandler<AsyncEvent<Connection>> remoteOpenEventHandler) {
        this.remoteOpenHandler = remoteOpenEventHandler;
        return this;
    }

    @Override
    public Connection closeEventHandler(EventHandler<AsyncEvent<Connection>> remoteCloseEventHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public Connection sessionOpenEventHandler(EventHandler<Session> remoteSessionOpenEventHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public Connection senderOpenEventHandler(EventHandler<Sender> remoteSenderOpenEventHandler) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public Connection receiverOpenEventHandler(EventHandler<Receiver> remoteReceiverOpenEventHandler) {
        // TODO Auto-generated method stub
        return this;
    }
}
