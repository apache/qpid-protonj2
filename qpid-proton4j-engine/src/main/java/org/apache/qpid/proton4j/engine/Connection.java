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
package org.apache.qpid.proton4j.engine;

import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;

/**
 * AMQP Connection state container
 *
 * TODO - Better Document the Connection APIs
 */
public interface Connection {

    /**
     * Open the end point.
     */
    public void open();

    /**
     * Close the end point
     */
    public void close();

    /**
     * @return the {@link Context} instance that is associated with this {@link Connection}
     */
    Context getContext();

    /**
     * @return the local endpoint state
     */
    public ConnectionState getLocalState();

    /**
     * @return the local endpoint error, or null if there is none
     */
    public ErrorCondition getLocalCondition();

    /**
     * Sets the local {@link ErrorCondition} to be applied to a {@link Connection} close.
     *
     * @param condition
     *      The error condition to convey to the remote peer on connection close.
     */
    public void setLocalCondition(ErrorCondition condition);

    //----- Operations on local end of this Connection

    /**
     * Creates a new Session linked to this Connection
     */
    Session session();

    /**
     * @returns the Container ID assigned to this Connection
     */
    String getContainerId();

    /**
     * Sets the Container Id to be used when opening this Connection.
     *
     * @param containerId
     *      The Container Id used for this end of the Connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    void setContainerId(String containerId);

    /**
     * Set the name of the host (either fully qualified or relative) to which
     * this connection is connecting to.  This information may be used by the
     * remote peer to determine the correct back-end service to connect the
     * client to.  This value will be sent in the Open performative.
     *
     * <b>Note that it is illegal to set the hostname to a numeric IP
     * address or include a port number.</b>
     *
     * @param hostname the RFC1035 compliant host name.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    public void setHostname(String hostname);

    /**
     * @return returns the host name assigned to this Connection.
     *
     * @see #setHostname
     */
    public String getHostname();

    /**
     * Set the channel max value for this Connection.
     *
     * @param channelMax
     *      The value to set for channel max when opening the connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    public void setChannelMax(int channelMax);

    /**
     * @return the currently configured channel max for this {@link Connection}
     */
    public int getChannelMax();

    /**
     * Set the idle timeout value for this Connection.
     *
     * @param idleTimeout
     *      The value to set for the idle timeout when opening the connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    public void setIdleTimeout(int idleTimeout);

    /**
     * @return the currently configured idle timeout for this {@link Connection}
     */
    public int getIdleTimeout();

    /**
     * Sets the capabilities to be offered on to the remote when this Connection is
     * opened.
     *
     * @param capabilities
     *      The capabilities to be offered to the remote when the Connection is opened.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    void setOfferedCapabilities(Symbol[] capabilities);

    /**
     * @return the configured capabilities that are offered to the remote when the Connection is opened.
     */
    Symbol[] getOfferedCapabilities();

    /**
     * Sets the capabilities that are desired from the remote when this Connection is
     * opened.
     *
     * @param capabilities
     *      The capabilities desired from the remote when the Connection is opened.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    void setDesiredCapabilities(Symbol[] capabilities);

    /**
     * @return the configured desired capabilities that are sent to the remote when the Connection is opened.
     */
    Symbol[] getDesiredCapabilities();

    /**
     * Sets the properties to be sent to the remote when this Connection is Opened.
     *
     * @param properties
     *      The properties that will be sent to the remote when this Connection is opened.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    void setProperties(Map<Symbol, Object> properties);

    /**
     * @return the configured properties sent to the remote when this Connection is opened.
     */
    Map<Symbol, Object> getProperties();

    //----- View state of remote end of this Connection

    /**
     * @return the Container Id assigned to the remote end of the Connection.
     */
    String getRemoteContainerId();

    /**
     * @return the host name assigned to the remote end of this Connection.
     */
    String getRemoteHostname();

    /**
     * @return the capabilities offered by the remote when it opened its end of the Connection.
     */
    Symbol[] getRemoteOfferedCapabilities();

    /**
     * @return the capabilities desired by the remote when it opened its end of the Connection.
     */
    Symbol[] getRemoteDesiredCapabilities();

    /**
     * @return the properties sent by the remote when it opened its end of the Connection.
     */
    Map<Symbol, Object> getRemoteProperties();

    /**
     * @return the remote state (as last communicated)
     */
    public ConnectionState getRemoteState();

    /**
     * @return the remote error, or null if there is none
     */
    public ErrorCondition getRemoteCondition();

    //----- Remote events for AMQP Connection resources

    /**
     * Sets a EventHandler for when an AMQP Open frame is received from the remote peer.
     *
     * Used to process remotely initiated Connections. Locally initiated sessions have their own EventHandler
     * invoked instead.  This method is typically used by servers to listen for the remote peer to open its
     * connection, while a client would listen for the server to open its end of the connection once a local open
     * has been performed.
     *
     * @param remoteOpenEventHandler
     *          the EventHandler that will be signaled when the connection has been remotely opened
     *
     * @return this connection
     */
    Connection openEventHandler(EventHandler<AsyncEvent<Connection>> remoteOpenEventHandler);

    /**
     * Sets a EventHandler for when an AMQP Close frame is received from the remote peer.
     *
     * @param remoteCloseEventHandler
     *          the EventHandler that will be signaled when the connection is remotely closed.
     *
     * @return this connection
     */
    Connection closeEventHandler(EventHandler<AsyncEvent<Connection>> remoteCloseEventHandler);

    /**
     * Sets a EventHandler for when an AMQP Begin frame is received from the remote peer.
     *
     * Used to process remotely initiated Sessions. Locally initiated sessions have their own EventHandler
     * invoked instead.  This method is Typically used by servers to listen for remote Session creation.
     *
     * @param remoteSessionOpenEventHandler
     *          the EventHandler that will be signaled when a session is remotely opened.
     *
     * @return this connection
     */
    Connection sessionOpenEventHandler(EventHandler<Session> remoteSessionOpenEventHandler);

    /**
     * Sets a EventHandler for when an AMQP Attach frame is received from the remote peer for a sending link.
     *
     * Used to process remotely initiated sending link.  Locally initiated links have their own EventHandler
     * invoked instead.  This method is Typically used by servers to listen for remote Receiver creation.
     * If an event handler for remote sender open is registered on the Session that the link is owned by then
     * that handler will be invoked instead of this one.
     *
     * @param remoteSenderOpenEventHandler
     *          the EventHandler that will be signaled when a sender link is remotely opened.
     *
     * @return this connection
     */
    Connection senderOpenEventHandler(EventHandler<Sender> remoteSenderOpenEventHandler);

    /**
     * Sets a EventHandler for when an AMQP Attach frame is received from the remote peer for a receiving link.
     *
     * Used to process remotely initiated receiving link.  Locally initiated links have their own EventHandler
     * invoked instead.  This method is Typically used by servers to listen for remote Sender creation.
     * If an event handler for remote receiver open is registered on the Session that the link is owned by then
     * that handler will be invoked instead of this one.
     *
     * @param remoteReceiverOpenEventHandler
     *          the EventHandler that will be signaled when a receiver link is remotely opened.
     *
     * @return this connection
     */
    Connection receiverOpenEventHandler(EventHandler<Receiver> remoteReceiverOpenEventHandler);

}
