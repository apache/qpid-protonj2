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
import org.apache.qpid.proton4j.engine.impl.ProtonConnection;

/**
 * AMQP Session interface
 */
public interface Session {

    /**
     * Open the end point.
     *
     * @return this Session
     */
    Session open();

    /**
     * Close the end point
     *
     * @return this Session
     */
    Session close();

    /**
     * @return the {@link Context} instance that is associated with this {@link Connection}
     */
    Context getContext();

    /**
     * @return the local session state
     */
    SessionState getLocalState();

    /**
     * @return the local endpoint error, or null if there is none
     */
    ErrorCondition getLocalCondition();

    /**
     * Sets the local {@link ErrorCondition} to be applied to a {@link Session} close.
     *
     * @param condition
     *      The error condition to convey to the remote peer on session close.
     *
     * @return this Session
     */
    Session setLocalCondition(ErrorCondition condition);

    /**
     * @return the parent {@link Connection} for this Session.
     */
    Connection getConnection();

    //----- Session sender and receiver factory methods

    /**
     * Create a new {@link Sender} link using the provided name.
     *
     * @param name
     *      The name to assign to the created {@link Sender}
     *
     * @return a newly created {@link Sender} instance.
     */
    Sender sender(String name);

    /**
     * Create a new {@link Receiver} link using the provided name
     *
     * @param name
     *      The name to assign to the created {@link Receiver}
     *
     * @return a newly created {@link Receiver} instance.
     */
    Receiver receiver(String name);

    //----- Configure the local end of the Session

    /**
     * Sets the maximum number of bytes this session can be sent from the remote.
     *
     * @param incomingCapacity
     *      maximum number of incoming bytes this session will allow
     *
     * @return this Session
     */
    Session setIncomingCapacity(int incomingCapacity);

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
     */
    Session setProperties(Map<Symbol, Object> properties);

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
     */
    Session setOfferedCapabilities(Symbol[] offeredCapabilities);

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
     */
    Session setDesiredCapabilities(Symbol[] desiredCapabilities);

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
     * Sets a {@link EventHandler} for when an AMQP Begin frame is received from the remote peer for this
     * {@link Session} which would have been locally opened previously.
     *
     * Typically used by clients, servers rely on {@link ProtonConnection#sessionOpenEventHandler(EventHandler)}.
     *
     * @param remoteOpenHandler
     *      The {@link EventHandler} to notify when this session is remotely opened.
     *
     * @return the session for chaining.
     */
    Session openHandler(EventHandler<Session> remoteOpenHandler);

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
     * Sets a EventHandler for when an AMQP Attach frame is received from the remote peer for a sending link.
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
    Session senderOpenEventHandler(EventHandler<Sender> remoteSenderOpenEventHandler);

    /**
     * Sets a EventHandler for when an AMQP Attach frame is received from the remote peer for a receiving link.
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
    Session receiverOpenEventHandler(EventHandler<Receiver> remoteReceiverOpenEventHandler);

}
