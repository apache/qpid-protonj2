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
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.apache.qpid.proton4j.engine.impl.ProtonSession;

/**
 * Base API for {@link Sender} and {@link Receiver} links.
 *
 * @param <T> The link type that this {@link Link} represents, {@link Sender} or {@link Receiver}
 */
public interface Link<T extends Link<T>> {

    /**
     * Open this end of the link
     *
     * @return this Link.
     *
     * @throws IllegalStateException if the underlying {@link Session} has already been closed.
     * @throws EngineStateException if an error occurs opening the Link or the Engine is shutdown.
     */
    T open() throws IllegalStateException, EngineStateException;

    /**
     * Close the end point locally and send the Close performative immediately if possible or holds it
     * until the Session / Connection / Engine state allows it.  If the engine encounters an error writing
     * the performative or the engine is in a failed state from a previous error then this method will
     * throw an exception.  If the engine has been shutdown then this method will close out the local
     * end of the {@link Link} and clean up any local resources before returning normally.
     *
     * @return this Link
     *
     * @throws EngineFailedException if an error occurs closing the {@link Link} or the Engine is in a failed state.
     */
    T close() throws EngineFailedException;

    /**
     * Detach this end of the link.
     *
     * @return this Link.
     *
     * @throws EngineStateException if an error occurs detaching the {@link Link} or the Engine is shutdown.
     */
    T detach();

    /**
     * Returns true if this {@link Link} is currently locally open meaning the state returned
     * from {@link Link#getState()} is equal to {@link LinkState#ACTIVE}.  A link
     * is locally opened after a call to {@link Link#open()} and before a call to
     * {@link Link#close()} or {@link Link#detach()}.
     *
     * @return true if the link is locally open.
     *
     * @see Session#isLocallyClosed()
     * @see Link#isLocallyDetached()
     */
    boolean isLocallyOpen();

    /**
     * Returns true if this {@link Link} is currently locally closed meaning the state returned
     * from {@link Link#getState()} is equal to {@link LinkState#CLOSED}.  A link
     * is locally closed after a call to {@link Link#close()}.
     *
     * @return true if the link is locally closed.
     *
     * @see Link#isLocallyOpen()
     * @see Link#isLocallyDetached()
     */
    boolean isLocallyClosed();

    /**
     * Returns true if this {@link Link} is currently locally detached meaning the state returned
     * from {@link Link#getState()} is equal to {@link LinkState#DETACHED}.  A link
     * is locally detached after a call to {@link Link#detach()}.
     *
     * @return true if the link is locally closed.
     *
     * @see Link#isLocallyOpen()
     * @see Link#isLocallyClosed()
     */
    boolean isLocallyDetached();

    /**
     * @return the {@link Context} instance that is associated with this {@link Connection}
     */
    Context getContext();

    /**
     * @return the local link state
     */
    LinkState getState();

    /**
     * @return the local endpoint error, or null if there is none
     */
    ErrorCondition getCondition();

    /**
     * Sets the local {@link ErrorCondition} to be applied to a {@link Link} close.
     *
     * @param condition
     *      The error condition to convey to the remote peer on link close or detach.
     *
     * @return this Link.
     */
    T setCondition(ErrorCondition condition);

    /**
     * Get the credit that is currently available or assigned to this link.
     *
     * @return the current link credit.
     */
    int getCredit();

    boolean isDrain();

    /**
     * @return the {@link Role} that this end of the link is performing.
     */
    Role getRole();

    /**
     * @return true if this link is acting in a sender {@link Role}.
     */
    boolean isSender();

    /**
     * @return true if this link is acting in a receiver {@link Role}.
     */
    boolean isReceiver();

    /**
     * @return the parent {@link Connection} for the {@link Link}
     */
    Connection getConnection();

    /**
     * @return the parent {@link Session} of the {@link Link}
     */
    Session getSession();

    /**
     * @return the parent {@link Engine} for this {@link Link}
     */
    Engine getEngine();

    /**
     * @return the link name that is assigned to this {@link Link}
     */
    String getName();

    /**
     * Sets the sender settle mode.
     *
     * Should only be called during link set-up, i.e. before calling {@link #open()}.
     *
     * If this endpoint is the initiator of the link, this method can be used to set a value other than
     * the default.
     *
     * If this endpoint is not the initiator, this method should be used to set a local value. According
     * to the AMQP spec, the application may choose to accept the sender's suggestion
     * (accessed by calling {@link #getRemoteSenderSettleMode()}) or choose another value. The value
     * has no effect on Proton, but may be useful to the application at a later point.
     *
     * In order to be AMQP compliant the application is responsible for honoring the settlement mode. See {@link Link}.
     *
     * @param senderSettleMode
     *      The {@link SenderSettleMode} that will be set on the local end of this link.
     *
     * @return this Link.
     *
     * @throws IllegalStateException if the {@link Link} has already been opened.
     */
    T setSenderSettleMode(SenderSettleMode senderSettleMode) throws IllegalStateException;

    /**
     * Gets the local link sender settlement mode.
     *
     * @return the local sender settlement mode, or null if none was set.
     *
     * @see #setSenderSettleMode(SenderSettleMode)
     */
    SenderSettleMode getSenderSettleMode();

    /**
     * Sets the receiver settle mode.
     *
     * Should only be called during link set-up, i.e. before calling {@link #open()}.
     *
     * If this endpoint is the initiator of the link, this method can be used to set a value other than
     * the default.
     *
     * Used in analogous way to {@link #setSenderSettleMode(SenderSettleMode)}
     *
     * @param receiverSettleMode
     *      The {@link ReceiverSettleMode} that will be set on the local end of this link.
     *
     * @return this Link.
     *
     * @throws IllegalStateException if the {@link Link} has already been opened.
     */
    T setReceiverSettleMode(ReceiverSettleMode receiverSettleMode) throws IllegalStateException;

    /**
     * Gets the local link receiver settlement mode.
     *
     * @return the local receiver settlement mode, or null if none was set.
     *
     * @see #setReceiverSettleMode(ReceiverSettleMode)
     */
    ReceiverSettleMode getReceiverSettleMode();

    /**
     * Sets the {@link Source} to assign to the local end of this {@link Link}.
     *
     * Must be called during link setup, i.e. before calling the {@link #open()} method.
     *
     * @param source
     *      The {@link Source} that will be set on the local end of this link.
     *
     * @return this Link.
     *
     * @throws IllegalStateException if the {@link Link} has already been opened.
     */
    T setSource(Source source) throws IllegalStateException;

    /**
     * @return the {@link Source} for the local end of this link.
     */
    Source getSource();

    /**
     * Sets the {@link Target} to assign to the local end of this {@link Link}.
     *
     * Must be called during link setup, i.e. before calling the {@link #open()} method.
     *
     * @param target
     *      The {@link Target} that will be set on the local end of this link.
     *
     * @return this Link.
     *
     * @throws IllegalStateException if the {@link Link} has already been opened.
     */
    T setTarget(Target target) throws IllegalStateException;

    /**
     * @return the {@link Target} for the local end of this link.
     */
    Target getTarget();

    /**
     * Gets the local link properties.
     *
     * @return a {@link Map} containing the properties currently set on this link.
     *
     * @see #setProperties(Map)
     */
    Map<Symbol, Object> getProperties();

    /**
     * Sets the local {@link Link} properties, to be conveyed to the peer via the Attach frame when
     * opening the local end of the link.
     *
     * Must be called during link setup, i.e. before calling the {@link #open()} method.
     *
     * @param properties
     *          the properties map to send, or null for none.
     *
     * @return this Link.
     *
     * @throws IllegalStateException if the {@link Link} has already been opened.
     */
    T setProperties(Map<Symbol, Object> properties) throws IllegalStateException;

    /**
     * Sets the local link offered capabilities, to be conveyed to the peer via the Attach frame
     * when attaching the link to the session.
     *
     * Must be called during link setup, i.e. before calling the {@link #open()} method.
     *
     * @param offeredCapabilities
     *          the offered capabilities array to send, or null for none.
     *
     * @return this Link.
     *
     * @throws IllegalStateException if the {@link Link} has already been opened.
     */
    T setOfferedCapabilities(Symbol... offeredCapabilities) throws IllegalStateException;

    /**
     * Gets the local link offered capabilities.
     *
     * @return the offered capabilities array, or null if none was set.
     *
     * @see #setOfferedCapabilities(Symbol[])
     */
    Symbol[] getOfferedCapabilities();

    /**
     * Sets the local link desired capabilities, to be conveyed to the peer via the Attach frame
     * when attaching the link to the session.
     *
     * Must be called during link setup, i.e. before calling the {@link #open()} method.
     *
     * @param desiredCapabilities
     *          the desired capabilities array to send, or null for none.
     *
     * @return this Link.
     *
     * @throws IllegalStateException if the {@link Link} has already been opened.
     */
    T setDesiredCapabilities(Symbol... desiredCapabilities) throws IllegalStateException;

    /**
     * Gets the local link desired capabilities.
     *
     * @return the desired capabilities array, or null if none was set.
     *
     * @see #setDesiredCapabilities(Symbol[])
     */
    Symbol[] getDesiredCapabilities();

    /**
     * Sets the local link max message size, to be conveyed to the peer via the Attach frame
     * when attaching the link to the session. Null or 0 means no limit.
     *
     * Must be called during link setup, i.e. before calling the {@link #open()} method.
     *
     * @param maxMessageSize
     *            the local max message size value, or null to clear. 0 also means no limit.
     *
     * @return this Link.
     *
     * @throws IllegalStateException if the {@link Link} has already been opened.
     */
    T setMaxMessageSize(UnsignedLong maxMessageSize) throws IllegalStateException;

    /**
     * Gets the local link max message size.
     *
     * @return the local max message size, or null if none was set. 0 also means no limit.
     *
     * @see #setMaxMessageSize(UnsignedLong)
     */
    UnsignedLong getMaxMessageSize();

    //----- View of the state of the link at the remote

    /**
     * Returns true if this {@link Link} is currently remotely open meaning the state returned
     * from {@link Link#getRemoteState()} is equal to {@link LinkState#ACTIVE}.  A link
     * is remotely opened after an {@link Attach} has been received from the remote and before a
     * {@link Detach} has been received from the remote.
     *
     * @return true if the link is remotely open.
     *
     * @see Link#isRemotelyClosed()
     */
    boolean isRemotelyOpen();

    /**
     * Returns true if this {@link Link} is currently remotely closed meaning the state returned
     * from {@link Link#getRemoteState()} is equal to {@link LinkState#CLOSED}.  A link
     * is remotely closed after an {@link Detach} has been received from the remote with the close
     * flag equal to true.
     *
     * @return true if the link is remotely closed.
     *
     * @see Link#isRemotelyOpen()
     * @see Link#isRemotelyDetached()
     */
    boolean isRemotelyClosed();

    /**
     * Returns true if this {@link Link} is currently remotely detached meaning the state returned
     * from {@link Link#getRemoteState()} is equal to {@link LinkState#DETACHED}.  A link
     * is remotely detached after an {@link Detach} has been received from the remote with the close
     * flag equal to false.
     *
     * @return true if the link is remotely detached.
     *
     * @see Link#isRemotelyOpen()
     * @see Link#isRemotelyClosed()
     */
    boolean isRemotelyDetached();

    /**
     * @return the {@link Source} for the remote end of this link.
     */
    Source getRemoteSource();

    /**
     * @return the {@link Target} for the remote end of this link.
     */
    Target getRemoteTarget();

    /**
     * Gets the remote link sender settlement mode, as conveyed from the peer via the Attach frame
     * when attaching the link to the session.
     *
     * @return the sender settlement mode conveyed by the peer, or null if there was none.
     *
     * @see #setSenderSettleMode(SenderSettleMode)
     */
    SenderSettleMode getRemoteSenderSettleMode();

    /**
     * Gets the remote link receiver settlement mode, as conveyed from the peer via the Attach frame
     * when attaching the link to the session.
     *
     * @return the sender receiver mode conveyed by the peer, or null if there was none.
     *
     * @see #setReceiverSettleMode(ReceiverSettleMode)
     */
    ReceiverSettleMode getRemoteReceiverSettleMode();

    /**
     * Gets the remote link offered capabilities, as conveyed from the peer via the Attach frame
     * when attaching the link to the session.
     *
     * @return the offered capabilities array conveyed by the peer, or null if there was none.
     */
    Symbol[] getRemoteOfferedCapabilities();

    /**
     * Gets the remote link desired capabilities, as conveyed from the peer via the Attach frame
     * when attaching the link to the session.
     *
     * @return the desired capabilities array conveyed by the peer, or null if there was none.
     */
    Symbol[] getRemoteDesiredCapabilities();

    /**
     * Gets the remote link properties, as conveyed from the peer via the Attach frame
     * when attaching the link to the session.
     *
     * @return the properties Map conveyed by the peer, or null if there was none.
     */
    Map<Symbol, Object> getRemoteProperties();

    /**
     * Gets the remote link max message size, as conveyed from the peer via the Attach frame
     * when attaching the link to the session.
     *
     * @return the remote max message size conveyed by the peer, or null if none was set. 0 also means no limit.
     */
    UnsignedLong getRemoteMaxMessageSize();

    /**
     * @return the remote link state (as last communicated)
     */
    LinkState getRemoteState();

    /**
     * @return the remote endpoint error, or null if there is none
     */
    ErrorCondition getRemoteCondition();

    //----- Remote events for AMQP Link resources

    /**
     * Sets a {@link EventHandler} for when an this link is opened locally via a call to {@link Link#open()}
     *
     * Typically used by clients for logging or other state update event processing.  Clients should not perform any
     * blocking calls within this context.  It is an error for the handler to throw an exception and the outcome of
     * doing so is undefined.
     *
     * @param localOpenHandler
     *      The {@link EventHandler} to notify when this link is locally opened.
     *
     * @return the link for chaining.
     */
    T localOpenHandler(EventHandler<T> localOpenHandler);

    /**
     * Sets a {@link EventHandler} for when an this link is closed locally via a call to {@link Link#close()}
     *
     * Typically used by clients for logging or other state update event processing.  Clients should not perform any
     * blocking calls within this context.  It is an error for the handler to throw an exception and the outcome of
     * doing so is undefined.
     *
     * @param localCloseHandler
     *      The {@link EventHandler} to notify when this link is locally closed.
     *
     * @return the link for chaining.
     */
    T localCloseHandler(EventHandler<T> localCloseHandler);

    /**
     * Sets a {@link EventHandler} for when an this link is detached locally via a call to {@link Link#detach()}
     *
     * Typically used by clients for logging or other state update event processing.  Clients should not perform any
     * blocking calls within this context.  It is an error for the handler to throw an exception and the outcome of
     * doing so is undefined.
     *
     * @param localDetachHandler
     *      The {@link EventHandler} to notify when this link is locally detached.
     *
     * @return the link for chaining.
     */
    T localDetachHandler(EventHandler<T> localDetachHandler);

    /**
     * Sets a {@link EventHandler} for when an AMQP Begin frame is received from the remote peer for this
     * {@link Link} which would have been locally opened previously.
     *
     * Typically used by clients, servers rely on {@link ProtonSession#senderOpenHandler(EventHandler)} and
     * {@link ProtonSession#receiverOpenHandler(EventHandler)}.
     *
     * @param remoteOpenHandler
     *      The {@link EventHandler} to notify when this link is remotely opened.
     *
     * @return the link for chaining.
     */
    T openHandler(EventHandler<T> remoteOpenHandler);

    /**
     * Sets a {@link EventHandler} for when an AMQP Detach frame is received from the remote peer for this
     * {@link Link} which would have been locally opened previously, the Detach from would have been marked
     * as not having been closed.
     *
     * @param remoteDetachHandler
     *      The {@link EventHandler} to notify when this link is remotely closed.
     *
     * @return the link for chaining.
     */
    T detachHandler(EventHandler<T> remoteDetachHandler);

    /**
     * Sets a {@link EventHandler} for when an AMQP Detach frame is received from the remote peer for this
     * {@link Link} which would have been locally opened previously, the detach would have been marked as
     * having been closed.
     *
     * @param remoteCloseHandler
     *      The {@link EventHandler} to notify when this link is remotely closed.
     *
     * @return the link for chaining.
     */
    T closeHandler(EventHandler<T> remoteCloseHandler);

    /**
     * Sets an {@link EventHandler} that is called when the parent {@link Session} is locally closed.
     *
     * Used to trigger link related cleanup work should the parent {@link Session} be closed before
     * this {@link Link} instance.  It is not required that an application deal with this event but it
     * can provide a useful notification that this link and its associated resources should now be closed
     * and cleaned up.
     *
     * @param sessionClosedEventHandler
     *
     * @return the link for chaining.
     */
    T sessionClosedHandler(EventHandler<T> sessionClosedEventHandler);

    /**
     * Sets an {@link EventHandler} that is invoked when the engine that supports this {@link Link} is shutdown
     * via a call to {@link Engine#shutdown()} which indicates a desire to terminate all engine operations. Any
     * link that has been both locally and remotely closed will not receive this event as it will no longer be
     * tracked by the parent {@link Session}.
     *
     * A typical use of this event would be from a locally closed {@link Link} that is awaiting response from
     * the remote.  If this event fires then there will never be a remote response and the client or server instance
     * should react accordingly to clean up any related session resources etc.
     *
     * @param engineShutdownEventHandler
     *      the EventHandler that will be signaled when this Link's engine is explicitly shutdown.
     *
     * @return this Link
     */
    T engineShutdownHandler(EventHandler<Engine> engineShutdownEventHandler);

}
