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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.End;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.apache.qpid.proton4j.engine.impl.ProtonConnection;

/**
 * AMQP Session interface
 */
public interface Session {

    /**
     * Open the end point.
     *
     * @return this Session
     *
     * @throws IllegalStateException if the underlying {@link Connection} has already been closed.
     * @throws EngineStateException if an error occurs opening the Session or the Engine is shutdown.
     */
    Session open() throws IllegalStateException, EngineStateException;

    /**
     * Close the end point locally and send the Close performative immediately if possible or holds it
     * until the Session / Connection / Engine state allows it.  If the engine encounters an error writing
     * the performative or the engine is in a failed state from a previous error then this method will
     * throw an exception.  If the engine has been shutdown then this method will close out the local
     * end of the session and clean up any local resources before returning normally.
     *
     * @return this Session
     *
     * @throws EngineFailedException if an error occurs closing the Session or the Engine is in a failed state.
     */
    Session close() throws EngineFailedException;

    /**
     * @return the {@link Context} instance that is associated with this {@link Connection}
     */
    Context getContext();

    /**
     * @return the local session state
     */
    SessionState getState();

    /**
     * @return the local endpoint error, or null if there is none
     */
    ErrorCondition getCondition();

    /**
     * Sets the local {@link ErrorCondition} to be applied to a {@link Session} close.
     *
     * @param condition
     *      The error condition to convey to the remote peer on session close.
     *
     * @return this Session
     */
    Session setCondition(ErrorCondition condition);

    /**
     * @return the parent {@link Connection} for this Session.
     */
    Connection getConnection();

    /**
     * @return the parent {@link Engine} for this Session.
     */
    Engine getEngine();

    /**
     * Returns true if this {@link Session} is currently locally open meaning the state returned
     * from {@link Session#getState()} is equal to {@link SessionState#ACTIVE}.  A session
     * is locally opened after a call to {@link Session#open()} and before a call to
     * {@link Session#close()}.
     *
     * @return true if the session is locally open.
     *
     * @see Session#isLocallyClosed()
     */
    boolean isLocallyOpen();

    /**
     * Returns true if this {@link Session} is currently locally closed meaning the state returned
     * from {@link Session#getState()} is equal to {@link SessionState#CLOSED}.  A session
     * is locally closed after a call to {@link Session#close()}.
     *
     * @return true if the session is locally closed.
     *
     * @see Session#isLocallyOpen()
     */
    boolean isLocallyClosed();

    /**
     * Returns a {@link Set} of all {@link Sender} and {@link Receiver} instances that are being tracked by
     * this {@link Session}.
     *
     * @return a set of Sender and Receiver instances tracked by this session.
     */
    Set<Link<?>> links();

    /**
     * Returns a {@link Set} of {@link Sender} instances that are being tracked by this {@link Session}.
     *
     * @return a set of Sender instances tracked by this session.
     */
    Set<Sender> senders();

    /**
     * Returns a {@link Set} of {@link Receiver} instances that are being tracked by this {@link Session}.
     *
     * @return a set of Receiver instances tracked by this session.
     */
    Set<Receiver> receivers();

    //----- Session sender and receiver factory methods

    /**
     * Create a new {@link Sender} link using the provided name.
     *
     * @param name
     *      The name to assign to the created {@link Sender}
     *
     * @return a newly created {@link Sender} instance.
     *
     * @throws IllegalStateException if the {@link Session} has already been closed.
     */
    Sender sender(String name) throws IllegalStateException;

    /**
     * Create a new {@link Receiver} link using the provided name
     *
     * @param name
     *      The name to assign to the created {@link Receiver}
     *
     * @return a newly created {@link Receiver} instance.
     *
     * @throws IllegalStateException if the {@link Session} has already been closed.
     */
    Receiver receiver(String name) throws IllegalStateException;

    //----- Configure the local end of the Session

    /**
     * Sets the maximum number of bytes this session can be sent from the remote.
     *
     * @param incomingCapacity
     *      maximum number of incoming bytes this session will allow
     *
     * @return this Session
     *
     * @throws IllegalStateException if the {@link Session} has already been closed.
     */
    Session setIncomingCapacity(int incomingCapacity) throws IllegalStateException;

    /**
     * @return the current incoming capacity of this session.
     */
    int getIncomingCapacity();

    /**
     * Sets the local session properties, to be conveyed to the peer via the Begin frame when
     * attaching the session to the session.
     *
     * Must be called during session setup, i.e. before calling the {@link #open()} method.
     *
     * @param properties
     *          the properties map to send, or null for none.
     *
     * @return this Session
     *
     * @throws IllegalStateException if the {@link Session} has already been opened.
     */
    Session setProperties(Map<Symbol, Object> properties) throws IllegalStateException;

    /**
     * Gets the local session properties.
     *
     * @return the properties map, or null if none was set.
     *
     * @see #setProperties(Map)
     */
    Map<Symbol, Object> getProperties();

    /**
     * Sets the local session offered capabilities, to be conveyed to the peer via the Begin frame
     * when opening the session.
     *
     * Must be called during session setup, i.e. before calling the {@link #open()} method.
     *
     * @param offeredCapabilities
     *          the offered capabilities array to send, or null for none.
     *
     * @return this Session
     *
     * @throws IllegalStateException if the {@link Session} has already been opened.
     */
    Session setOfferedCapabilities(Symbol... offeredCapabilities) throws IllegalStateException;

    /**
     * Gets the local session offered capabilities.
     *
     * @return the offered capabilities array, or null if none was set.
     *
     * @see #setOfferedCapabilities(Symbol[])
     */
    Symbol[] getOfferedCapabilities();

    /**
     * Sets the local session desired capabilities, to be conveyed to the peer via the Begin frame
     * when opening the session.
     *
     * Must be called during session setup, i.e. before calling the {@link #open()} method.
     *
     * @param desiredCapabilities
     *          the desired capabilities array to send, or null for none.
     *
     * @return this Session
     *
     * @throws IllegalStateException if the {@link Session} has already been opened.
     */
    Session setDesiredCapabilities(Symbol... desiredCapabilities) throws IllegalStateException;

    /**
     * Gets the local session desired capabilities.
     *
     * @return the desired capabilities array, or null if none was set.
     *
     * @see #setDesiredCapabilities(Symbol[])
     */
    Symbol[] getDesiredCapabilities();

    //----- View the remote end of the Session configuration

    /**
     * Returns true if this {@link Session} is currently remotely open meaning the state returned
     * from {@link Session#getRemoteState()} is equal to {@link SessionState#ACTIVE}.  A session
     * is remotely opened after an {@link Begin} has been received from the remote and before a {@link End}
     * has been received from the remote.
     *
     * @return true if the session is remotely open.
     *
     * @see Session#isRemotelyClosed()
     */
    boolean isRemotelyOpen();

    /**
     * Returns true if this {@link Session} is currently remotely closed meaning the state returned
     * from {@link Session#getRemoteState()} is equal to {@link SessionState#CLOSED}.  A session
     * is remotely closed after an {@link End} has been received from the remote.
     *
     * @return true if the session is remotely closed.
     *
     * @see Session#isRemotelyOpen()
     */
    boolean isRemotelyClosed();

    /**
     * Gets the remote session offered capabilities, as conveyed from the peer via the Begin frame
     * when opening the session.
     *
     * @return the offered capabilities array conveyed by the peer, or null if there was none.
     */
    Symbol[] getRemoteOfferedCapabilities();

    /**
     * Gets the remote session desired capabilities, as conveyed from the peer via the Begin frame
     * when opening the session.
     *
     * @return the desired capabilities array conveyed by the peer, or null if there was none.
     */
    Symbol[] getRemoteDesiredCapabilities();

    /**
     * Gets the remote session properties, as conveyed from the peer via the Begin frame
     * when opening the session.
     *
     * @return the properties Map conveyed by the peer, or null if there was none.
     */
    Map<Symbol, Object> getRemoteProperties();

    /**
     * @return the remote session state (as last communicated)
     */
    SessionState getRemoteState();

    /**
     * @return the remote endpoint error, or null if there is none
     */
    ErrorCondition getRemoteCondition();

    //----- Remote events for AMQP Session resources

    /**
     * Sets a {@link EventHandler} for when an this session is opened locally via a call to {@link Session#open()}
     *
     * Typically used by clients for logging or other state update event processing.  Clients should not perform any
     * blocking calls within this context.  It is an error for the handler to throw an exception and the outcome of
     * doing so is undefined.
     *
     * @param localOpenHandler
     *      The {@link EventHandler} to notify when this session is locally opened.
     *
     * @return the session for chaining.
     */
    Session localOpenHandler(EventHandler<Session> localOpenHandler);

    /**
     * Sets a {@link EventHandler} for when an AMQP Begin frame is received from the remote peer for this
     * {@link Session} which would have been locally opened previously.
     *
     * Typically used by clients, servers rely on {@link ProtonConnection#sessionOpenHandler(EventHandler)}.
     *
     * @param remoteOpenHandler
     *      The {@link EventHandler} to notify when this session is remotely opened.
     *
     * @return the session for chaining.
     */
    Session openHandler(EventHandler<Session> remoteOpenHandler);

    /**
     * Sets a {@link EventHandler} for when an this session is closed locally via a call to {@link Session#close()}
     *
     * Typically used by clients for logging or other state update event processing.  Clients should not perform any
     * blocking calls within this context.  It is an error for the handler to throw an exception and the outcome of
     * doing so is undefined.
     *
     * @param localCloseHandler
     *      The {@link EventHandler} to notify when this session is locally closed.
     *
     * @return the session for chaining.
     */
    Session localCloseHandler(EventHandler<Session> localCloseHandler);

    /**
     * Sets a {@link EventHandler} for when an AMQP End frame is received from the remote peer for this
     * {@link Session} which would have been locally opened previously.
     *
     * @param remoteCloseHandler
     *      The {@link EventHandler} to notify when this session is remotely closed.
     *
     * @return the session for chaining.
     */
    Session closeHandler(EventHandler<Session> remoteCloseHandler);

    /**
     * Sets a {@link EventHandler} for when an AMQP Attach frame is received from the remote peer for a sending link.
     *
     * Used to process remotely initiated sending link.  Locally initiated links have their own EventHandler
     * invoked instead.  This method is Typically used by servers to listen for remote Receiver creation.
     * If an event handler for remote sender open is registered on this Session for a link scoped to it then
     * this handler will be invoked instead of the variant in the Connection API.
     *
     * @param remoteSenderOpenEventHandler
     *          the EventHandler that will be signaled when a sender link is remotely opened.
     *
     * @return this session for chaining
     */
    Session senderOpenHandler(EventHandler<Sender> remoteSenderOpenEventHandler);

    /**
     * Sets a {@link EventHandler} for when an AMQP Attach frame is received from the remote peer for a receiving link.
     *
     * Used to process remotely initiated receiving link.  Locally initiated links have their own EventHandler
     * invoked instead.  This method is Typically used by servers to listen for remote Sender creation.
     * If an event handler for remote sender open is registered on this Session for a link scoped to it then
     * this handler will be invoked instead of the variant in the Connection API.
     *
     * @param remoteReceiverOpenEventHandler
     *          the EventHandler that will be signaled when a receiver link is remotely opened.
     *
     * @return this session for chaining
     */
    Session receiverOpenHandler(EventHandler<Receiver> remoteReceiverOpenEventHandler);

    /**
     * Sets an {@link EventHandler} that is invoked when the engine that create this {@link Session} is shutdown
     * via a call to {@link Engine#shutdown()} which indicates a desire to terminate all engine operations.  Any
     * session that has been both locally and remotely ended will not receive this event as it will no longer be
     * tracked by the parent {@link Connection}.
     *
     * A typical use of this event would be from a locally closed {@link Session} that is awaiting response from
     * the remote.  If this event fires then there will never be a remote response and the client or server instance
     * should react accordingly to clean up any related session resources etc.
     *
     * @param engineShutdownEventHandler
     *      the EventHandler that will be signaled when this Session's engine is explicitly shutdown.
     *
     * @return this Session
     */
    Session engineShutdownHandler(EventHandler<Engine> engineShutdownEventHandler);

}
