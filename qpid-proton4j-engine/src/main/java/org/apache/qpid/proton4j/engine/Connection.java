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
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.Close;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;

/**
 * AMQP Connection state container
 *
 * TODO - Better Document the Connection APIs
 */
public interface Connection {

    /**
     * Open the end point locally, sending the Open performative immediately if possible or holding
     * it until SASL negotiations or the AMQP header exchange has completed.
     *
     * @return this connection.
     *
     * @throws EngineStateException if an error occurs opening the Connection or the Engine is shutdown.
     */
    Connection open() throws EngineStateException;

    /**
     * Close the end point locally and send the Close performative immediately if possible or holds it
     * until the Connection / Engine state allows it.  If the engine encounters an error writing the
     * performative or the engine is in a failed state from a previous error then this method will
     * throw an exception.  If the engine has been shutdown then this method will close out the local
     * end of the connection and clean up any local resources before returning normally.
     *
     * @return this connection.
     *
     * @throws EngineFailedException if an error occurs closing the Connection or the Engine is in a failed state.
     */
    Connection close() throws EngineFailedException;

    /**
     * Convenience method which is the same as calling {@link Engine#tick(long)}.
     *
     * @param current
     *      Current time value usually taken from {@link System#nanoTime()}
     *
     * @return this {@link Connection} instance.
     *
     * @throws IllegalStateException if the {@link Engine} is already performing auto tick handling.
     * @throws EngineStateException if the Engine state precludes accepting new input.

     * @see Engine#tick(long)
     */
    Connection tick(long current) throws IllegalStateException, EngineStateException;

    /**
     * Convenience method which is the same as calling {@link Engine#tickAuto(ScheduledExecutorService)}.
     *
     * @param executor
     *      The single threaded execution context where all engine work takes place.
     *
     * @return this {@link Connection} instance.
     *
     * @throws IllegalStateException if the {@link Engine} is already performing auto tick handling.
     * @throws EngineStateException if the Engine state precludes accepting new input.
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
     * @return the local connection state only
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
     * Returns true if this {@link Connection} is currently locally open meaning the state returned
     * from {@link Connection#getState()} is equal to {@link ConnectionState#ACTIVE}.  A connection
     * is locally opened after a call to {@link Connection#open()} and before a call to
     * {@link Connection#close()}.
     *
     * @return true if the connection is locally open.
     *
     * @see Connection#isLocallyClosed()
     */
    boolean isLocallyOpen();

    /**
     * Returns true if this {@link Connection} is currently locally closed meaning the state returned
     * from {@link Connection#getState()} is equal to {@link ConnectionState#CLOSED}.  A connection
     * is locally closed after a call to {@link Connection#close()}.
     *
     * @return true if the connection is locally closed.
     *
     * @see Connection#isLocallyOpen()
     */
    boolean isLocallyClosed();

    /**
     * @return the Container ID assigned to this Connection
     */
    String getContainerId();

    /**
     * Sets the Container Id to be used when opening this Connection.  The container Id can only
     * be modified prior to a call to {@link Connection#open()}, once the connection has been
     * opened locally an error will be thrown if this method is called.
     *
     * @param containerId
     *      The Container Id used for this end of the Connection.
     *
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setContainerId(String containerId) throws IllegalStateException;

    /**
     * Set the name of the host (either fully qualified or relative) to which this
     * connection is connecting to.  This information may be used by the remote peer
     * to determine the correct back-end service to connect the client to. This value
     * will be sent in the Open performative.
     *
     * <b>Note that it is illegal to set the host name to a numeric IP
     * address or include a port number.</b>
     *
     * The host name value can only be modified prior to a call to {@link Connection#open()},
     * once the connection has been opened locally an error will be thrown if this method
     * is called.
     *
     * @param hostname the RFC1035 compliant host name.
     *
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setHostname(String hostname) throws IllegalStateException;

    /**
     * @return returns the host name assigned to this Connection.
     *
     * @see #setHostname
     */
    String getHostname();

    /**
     * Set the channel max value for this Connection.
     *
     * The channel max value can only be modified prior to a call to {@link Connection#open()},
     * once the connection has been opened locally an error will be thrown if this method
     * is called.
     *
     * @param channelMax
     *      The value to set for channel max when opening the connection.
     *
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setChannelMax(int channelMax) throws IllegalStateException;

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
     * The max frame size value can only be modified prior to a call to {@link Connection#open()},
     * once the connection has been opened locally an error will be thrown if this method
     * is called.
     *
     * @param maxFrameSize
     *      The maximum number of bytes allowed for a single
     *
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setMaxFrameSize(long maxFrameSize) throws IllegalStateException;

    /**
     * @return the currently configured max frame size this connection will accept.
     */
    long getMaxFrameSize();

    /**
     * Set the idle timeout value for this Connection.
     *
     * The idle timeout value can only be modified prior to a call to {@link Connection#open()},
     * once the connection has been opened locally an error will be thrown if this method
     * is called.
     *
     * @param idleTimeout
     *      The value to set for the idle timeout when opening the connection.
     *
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setIdleTimeout(long idleTimeout) throws IllegalStateException;

    /**
     * @return the currently configured idle timeout for this {@link Connection}
     */
    long getIdleTimeout();

    /**
     * Sets the capabilities to be offered on to the remote when this Connection is
     * opened.
     *
     * The offered capabilities value can only be modified prior to a call to {@link Connection#open()},
     * once the connection has been opened locally an error will be thrown if this method
     * is called.
     *
     * @param capabilities
     *      The capabilities to be offered to the remote when the Connection is opened.
     *
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setOfferedCapabilities(Symbol... capabilities) throws IllegalStateException;

    /**
     * @return the configured capabilities that are offered to the remote when the Connection is opened.
     */
    Symbol[] getOfferedCapabilities();

    /**
     * Sets the capabilities that are desired from the remote when this Connection is
     * opened.
     *
     * The desired capabilities value can only be modified prior to a call to {@link Connection#open()},
     * once the connection has been opened locally an error will be thrown if this method
     * is called.
     *
     * @param capabilities
     *      The capabilities desired from the remote when the Connection is opened.
     *
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setDesiredCapabilities(Symbol... capabilities) throws IllegalStateException;

    /**
     * @return the configured desired capabilities that are sent to the remote when the Connection is opened.
     */
    Symbol[] getDesiredCapabilities();

    /**
     * Sets the properties to be sent to the remote when this Connection is Opened.
     *
     * The connection properties value can only be modified prior to a call to {@link Connection#open()},
     * once the connection has been opened locally an error will be thrown if this method
     * is called.
     *
     * @param properties
     *      The properties that will be sent to the remote when this Connection is opened.
     *
     * @return this connection.
     *
     * @throws IllegalStateException if the Connection has already been opened.
     */
    Connection setProperties(Map<Symbol, Object> properties) throws IllegalStateException;

    /**
     * @return the configured properties sent to the remote when this Connection is opened.
     */
    Map<Symbol, Object> getProperties();

    //----- Session specific APIs for this Connection

    /**
     * Creates a new Session linked to this Connection
     *
     * @return a newly created {@link Session} linked to this {@link Connection}.
     *
     * @throws IllegalStateException if the {@link Connection} has already been closed.
     */
    Session session() throws IllegalStateException;

    /**
     * Returns an unmodifiable {@link Set} of Sessions that are tracked by the Connection.
     *
     * The {@link Session} instances returned from this method will be locally or remotely open or
     * both which gives the caller full view of the complete set of known {@link Session} instances.
     *
     * @return an unmodifiable {@link Set} of Sessions tracked by this Connection.
     */
    Set<Session> sessions();

    //----- View state of remote end of this Connection

    /**
     * Returns true if this {@link Connection} is currently remotely open meaning the state returned
     * from {@link Connection#getRemoteState()} is equal to {@link ConnectionState#ACTIVE}.  A connection
     * is remotely opened after an {@link Open} has been received from the remote and before a {@link Close}
     * has been received from the remote.
     *
     * @return true if the connection is remotely open.
     *
     * @see Connection#isRemotelyClosed()
     */
    boolean isRemotelyOpen();

    /**
     * Returns true if this {@link Connection} is currently remotely closed meaning the state returned
     * from {@link Connection#getRemoteState()} is equal to {@link ConnectionState#CLOSED}.  A connection
     * is remotely closed after an {@link Close} has been received from the remote.
     *
     * @return true if the connection is remotely closed.
     *
     * @see Connection#isRemotelyOpen()
     */
    boolean isRemotelyClosed();

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
     * Sets a {@link EventHandler} for when an this connection is opened locally via a call to {@link Connection#open()}
     *
     * Typically used by clients for logging or other state update event processing.  Clients should not perform any
     * blocking calls within this context.  It is an error for the handler to throw an exception and the outcome of
     * doing so is undefined.
     *
     * @param localOpenHandler
     *      The {@link EventHandler} to notify when this connection is locally opened.
     *
     * @return this connection
     */
    Connection localOpenHandler(EventHandler<Connection> localOpenHandler);

    /**
     * Sets a {@link EventHandler} for when an this connection is closed locally via a call to {@link Connection#close()}
     *
     * Typically used by clients for logging or other state update event processing.  Clients should not perform any
     * blocking calls within this context.  It is an error for the handler to throw an exception and the outcome of
     * doing so is undefined.
     *
     * @param localCloseHandler
     *      The {@link EventHandler} to notify when this connection is locally closed.
     *
     * @return this connection
     */
    Connection localCloseHandler(EventHandler<Connection> localCloseHandler);

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

    /**
     * Sets an {@link EventHandler} that is invoked when the engine that create this {@link Connection} is shutdown
     * via a call to {@link Engine#shutdown()} which indicates a desire to terminate all engine operations.
     *
     * @param engineShutdownEventHandler
     *      the EventHandler that will be signaled when this connection's engine is explicitly shutdown.
     *
     * @return this connection
     */
    Connection engineShutdownHandler(EventHandler<Engine> engineShutdownEventHandler);

}
