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
package org.apache.qpid.protonj2.engine;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.qpid.protonj2.engine.exceptions.EngineStateException;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;

/**
 * AMQP Connection state container
 */
public interface Connection extends Endpoint<Connection> {

    /**
     * If not already negotiated this method initiates the AMQP protocol negotiation phase of
     * the connection process sending the {@link AMQPHeader} to the remote peer.  For a client
     * application this could mean requesting the server to indicate if it supports the version
     * of the protocol this client speaks.  In rare cases a server could use this to preemptively
     * send its AMQP header.
     *
     * Once a header is sent the remote should respond with the AMQP Header that indicates what
     * protocol level it supports and if there is a mismatch the the engine will be failed with
     * a error indicating the protocol support was not successfully negotiated.
     *
     * If the engine has a configured SASL layer then by starting the AMQP Header exchange this
     * will implicitly first attempt the SASL authentication step of the connection process.
     *
     * @return this {@link Connection} instance.
     *
     * @throws EngineStateException if the Engine state precludes accepting new input.
     */
    Connection negotiate() throws EngineStateException;

    /**
     * If not already negotiated this method initiates the AMQP protocol negotiation phase of
     * the connection process sending the {@link AMQPHeader} to the remote peer.  For a client
     * application this could mean requesting the server to indicate if it supports the version
     * of the protocol this client speaks.  In rare cases a server could use this to preemptively
     * send its AMQP header.
     *
     * Once a header is sent the remote should respond with the AMQP Header that indicates what
     * protocol level it supports and if there is a mismatch the the engine will be failed with
     * a error indicating the protocol support was not successfully negotiated.
     *
     * If the engine has a configured SASL layer then by starting the AMQP Header exchange this
     * will implicitly first attempt the SASL authentication step of the connection process.
     *
     * The provided remote AMQP Header handler will be called once the remote sends its AMQP Header to
     * the either preemptively or as a response to offered AMQP Header from this peer, even if that has
     * already happened prior to this call.
     *
     * @param remoteAMQPHeaderHandler
     *      Handler to be called when an AMQP Header response has arrived.
     *
     * @return this {@link Connection} instance.
     *
     * @throws EngineStateException if the Engine state precludes accepting new input.
     */
    Connection negotiate(EventHandler<AMQPHeader> remoteAMQPHeaderHandler) throws EngineStateException;

    /**
     * Performs a tick operation on the connection which checks that Connection Idle timeout processing
     * is run.  This method is a convenience method that delegates the work to the {@link Engine#tick(long)}
     * method.
     *
     * It is an error to call this method if {@link Connection#tickAuto(ScheduledExecutorService)} was called.
     *
     * @param current
     *      Current time value usually taken from {@link System#nanoTime()}
     *
     * @return the absolute deadline in milliseconds to next call tick by/at, or 0 if there is none.
     *
     * @throws IllegalStateException if the {@link Engine} is already performing auto tick handling.
     * @throws EngineStateException if the Engine state precludes accepting new input.

     * @see Engine#tick(long)
     */
    long tick(long current) throws IllegalStateException, EngineStateException;

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
     * @return the local connection state only
     */
    ConnectionState getState();

    /**
     * @return this {@link Connection} as it is the root of the {@link Endpoint} hierarchy.
     */
    @Override
    Connection getParent();

    //----- Operations on local end of this Connection

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
     * @return the remote set max frame size limit.
     */
    long getRemoteMaxFrameSize();

    /**
     * @return the remote state (as last communicated)
     */
    ConnectionState getRemoteState();

    //----- Remote events for AMQP Connection resources

    /**
     * Sets a {@link EventHandler} for when an AMQP Begin frame is received from the remote peer.
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
     * Sets a {@link EventHandler} for when an AMQP Attach frame is received from the remote peer for a sending link.
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
     * Sets a {@link EventHandler} for when an AMQP Attach frame is received from the remote peer for a receiving link.
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
     * Sets a {@link EventHandler} for when an AMQP Attach frame is received from the remote peer for a transaction
     * coordination link.
     *
     * Used to process remotely initiated transaction manager link.  Locally initiated links have their own EventHandler
     * invoked instead.  This method is Typically used by servers to listen for remote {@link TransactionController}
     * creation.  If an event handler for remote {@link TransactionController} open is registered on the Session that the
     * link is owned by then that handler will be invoked instead of this one.
     *
     * @param remoteTxnManagerOpenEventHandler
     *          the EventHandler that will be signaled when a {@link TransactionController} link is remotely opened.
     *
     * @return this connection
     */
    Connection transactionManagerOpenHandler(EventHandler<TransactionManager> remoteTxnManagerOpenEventHandler);

}
