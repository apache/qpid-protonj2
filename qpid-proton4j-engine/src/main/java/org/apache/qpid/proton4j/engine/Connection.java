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
import java.util.concurrent.ScheduledExecutorService;

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
     *
     * @return this connection.
     */
    Connection open();

    /**
     * Close the end point
     *
     * @return this connection.
     */
    Connection close();

    /**
     * Convenience method which is the same as calling {@link Engine#tick(long)}.
     *
     * @param current
     *      Current time value usually taken from {@link System#nanoTime()}
     *
     * @return this {@link Connection} instance.
     *
     * @see Engine#tick(long)
     */
    Connection tick(long current);

    /**
     * Convenience method which is the same as calling {@link Engine#tickAuto(ScheduledExecutorService)}.
     *
     * @param executor
     *      The single threaded execution context where all engine work takes place.
     *
     * @return this {@link Connection} instance.
     *
     * @see Engine#tickAuto(ScheduledExecutorService)
     */
    Connection tickAuto(ScheduledExecutorService executor);

    /**
     * @return the {@link Context} instance that is associated with this {@link Connection}
     */
    Context getContext();

    /**
     * @return the {@link Engine} which created this {@link Connection} instance.
     */
    Engine getEngine();

    /**
     * @return the local connection state only/
     */
    ConnectionState getState();

    /**
     * @return the local endpoint error, or null if there is none
     */
    ErrorCondition getCondition();

    /**
     * Sets the local {@link ErrorCondition} to be applied to a {@link Connection} close.
     *
     * @param condition
     *      The error condition to convey to the remote peer on connection close.
     *
     * @return this connection.
     */
    Connection setCondition(ErrorCondition condition);

    //----- Operations on local end of this Connection

    /**
     * Creates a new Session linked to this Connection
     *
     * @return a newly created {@link Session} linked to this {@link Connection}.
     */
    Session session();

    /**
     * @return the Container ID assigned to this Connection
     */
    String getContainerId();

    /**
     * Sets the Container Id to be used when opening this Connection.
     *
     * @param containerId
     *      The Container Id used for this end of the Connection.
     *
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setContainerId(String containerId);

    /**
     * Set the name of the host (either fully qualified or relative) to which
     * this connection is connecting to.  This information may be used by the
     * remote peer to determine the correct back-end service to connect the
     * client to.  This value will be sent in the Open performative.
     *
     * <b>Note that it is illegal to set the host name to a numeric IP
     * address or include a port number.</b>
     *
     * @param hostname the RFC1035 compliant host name.
     *
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setHostname(String hostname);

    /**
     * @return returns the host name assigned to this Connection.
     *
     * @see #setHostname
     */
    String getHostname();

    /**
     * Set the channel max value for this Connection.
     *
     * @param channelMax
     *      The value to set for channel max when opening the connection.
     *
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setChannelMax(int channelMax);

    /**
     * @return the currently configured channel max for this {@link Connection}
     */
    int getChannelMax();

    /**
     * Sets the maximum frame size allowed for this connection, which is the largest single frame
     * that the remote can send to this {@link Connection} before it will close the connection with
     * an error condition indicating the violation.
     *
     * The legal range for this value is defined as (512 - 2^32-1) bytes.
     *
     * @param maxFrameSize
     *      The maximum number of bytes allowed for a single
     *
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setMaxFrameSize(long maxFrameSize);

    /**
     * @return the currently configured max frame size this connection will accept.
     */
    long getMaxFrameSize();

    /**
     * Set the idle timeout value for this Connection.
     *
     * @param idleTimeout
     *      The value to set for the idle timeout when opening the connection.
     *
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setIdleTimeout(long idleTimeout);

    /**
     * @return the currently configured idle timeout for this {@link Connection}
     */
    long getIdleTimeout();

    /**
     * Sets the capabilities to be offered on to the remote when this Connection is
     * opened.
     *
     * @param capabilities
     *      The capabilities to be offered to the remote when the Connection is opened.
     *
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setOfferedCapabilities(Symbol[] capabilities);

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
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setDesiredCapabilities(Symbol[] capabilities);

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
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setProperties(Map<Symbol, Object> properties);

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
     * @return the idle timeout value provided by the remote end of this Connection.
     */
    long getRemoteIdleTimeout();

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
    ConnectionState getRemoteState();

    /**
     * @return the remote error, or null if there is none
     */
    ErrorCondition getRemoteCondition();

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
    Connection openHandler(EventHandler<Connection> remoteOpenEventHandler);

    /**
     * Sets a EventHandler for when an AMQP Close frame is received from the remote peer.
     *
     * @param remoteCloseEventHandler
     *          the EventHandler that will be signaled when the connection is remotely closed.
     *
     * @return this connection
     */
    Connection closeHandler(EventHandler<Connection> remoteCloseEventHandler);

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
    Connection sessionOpenHandler(EventHandler<Session> remoteSessionOpenEventHandler);

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
    Connection senderOpenHandler(EventHandler<Sender> remoteSenderOpenEventHandler);

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
    Connection receiverOpenHandler(EventHandler<Receiver> remoteReceiverOpenEventHandler);

}
