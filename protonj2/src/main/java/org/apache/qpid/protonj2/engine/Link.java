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

import org.apache.qpid.protonj2.engine.exceptions.EngineStateException;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.messaging.Terminus;
import org.apache.qpid.protonj2.types.transactions.Coordinator;
import org.apache.qpid.protonj2.types.transport.Attach;
import org.apache.qpid.protonj2.types.transport.Detach;
import org.apache.qpid.protonj2.types.transport.Flow;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.Role;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;

/**
 * Base API for {@link Sender} and {@link Receiver} links.
 *
 * @param <L> The link type that this {@link Link} represents, {@link Sender} or {@link Receiver}
 */
public interface Link<L extends Link<L>> extends Endpoint<L> {

    /**
     * Detach this end of the link.
     *
     * @return this Link.
     *
     * @throws EngineStateException if an error occurs detaching the {@link Link} or the Engine is shutdown.
     */
    L detach();

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
     * Returns true if this {@link Link} is currently locally detached or locally closed meaning the
     * state returned from {@link Link#getState()} is equal to {@link LinkState#DETACHED} or
     * {@link LinkState#CLOSED}.  A link is locally detached after a call to {@link Link#detach()} and
     * is locally closed after a call to {@link Link#close()}.
     *
     * @return true if the link is locally closed or detached.
     *
     * @see Link#isLocallyOpen()
     * @see Link#isLocallyDetached()
     * @see Link#isLocallyClosed()
     */
    boolean isLocallyClosedOrDetached();

    /**
     * @return the local link state
     */
    LinkState getState();

    /**
     * Get the credit that is currently available or assigned to this link.
     *
     * @return the current link credit.
     */
    int getCredit();

    /**
     * Indicates if the link is draining. For a {@link Sender} link this indicates that the
     * remote has requested that the Sender transmit deliveries up to the currently available
     * credit or indicate that it has no more to send.  For a {@link Receiver} this indicates
     * that the Receiver has requested that the Sender consume its outstanding credit.
     *
     * @return true if the {@link Link} is currently marked as draining.
     */
    boolean isDraining();

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
     * @return the parent {@link Session} of the {@link Link}
     */
    @Override
    Session getParent();

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
    L setSenderSettleMode(SenderSettleMode senderSettleMode) throws IllegalStateException;

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
    L setReceiverSettleMode(ReceiverSettleMode receiverSettleMode) throws IllegalStateException;

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
    L setSource(Source source) throws IllegalStateException;

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
    L setTarget(Target target) throws IllegalStateException;

    /**
     * Sets the {@link Coordinator} target to assign to the local end of this {@link Link}.
     *
     * Must be called during link setup, i.e. before calling the {@link #open()} method.
     *
     * @param coordinator
     *      The {@link Coordinator} target that will be set on the local end of this link.
     *
     * @return this Link.
     *
     * @throws IllegalStateException if the {@link Link} has already been opened.
     */
    L setTarget(Coordinator coordinator) throws IllegalStateException;

    /**
     * Returns the currently set Target for this {@link Link}.  A link target can be either
     * a {@link Target} type for a {@link Sender} or {@link Receiver} link or if the link is
     * to be transaction resource then the target type will be a {@link Coordinator} instance.
     *
     * @return the link target {@link Terminus} for the local end of this link.
     */
    <T extends Terminus> T getTarget();

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
    L setMaxMessageSize(UnsignedLong maxMessageSize) throws IllegalStateException;

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
     * @see Link#isRemotelyDetached()
     */
    @Override
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
    @Override
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
     * Returns true if this {@link Link} is currently remotely detached or closed meaning the state
     * returned from {@link Link#getRemoteState()} is equal to {@link LinkState#DETACHED} or
     * {@link LinkState#CLOSED}.  A link is remotely detached or closed after a {@link Detach}
     * has been received from the remote.
     *
     * @return true if the link is remotely detached or closed.
     *
     * @see Link#isRemotelyOpen()
     * @see Link#isRemotelyClosed()
     * @see Link#isRemotelyDetached()
     */
    boolean isRemotelyClosedOrDetached();

    /**
     * @return the source {@link Terminus} for the remote end of this link.
     */
    Source getRemoteSource();

    /**
     * Returns the remote target {@link Terminus} cast to the given type.  This can be used when
     * the underlying type is known by the caller or as a control to validate the assumption of the
     * underlying type.
     *
     * the currently set Target for this {@link Link}.  A link target can be either a {@link Target}
     * type for a {@link Sender} or {@link Receiver} link or if the link is to be transaction resource
     * then the target type will be a {@link Coordinator} instance.
     *
     * @param <T>
     *      The type that the remote {@link Terminus} will be cast to on return.
     *
     * @return the source {@link Terminus} for the remote end of this link.
     */
    <T extends Terminus> T getRemoteTarget();

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

    //----- Remote events for AMQP Link resources

    /**
     * Sets a {@link EventHandler} for when an this link is detached locally via a call to {@link Link#detach()}
     *
     * This is a convenience event that supplements the normal {@link Endpoint#localCloseHandler(EventHandler)}
     * event point if set.  If no local detached event handler is set the endpoint will route the detached event
     * to the local closed event handler if set and allow it to process the event in one location.
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
    L localDetachHandler(EventHandler<L> localDetachHandler);

    /**
     * Sets a {@link EventHandler} for when an AMQP Detach frame is received from the remote peer for this
     * {@link Link} which would have been locally opened previously, the Detach from would have been marked
     * as not having been closed.
     *
     * This is a convenience event that supplements the normal {@link Endpoint#closeHandler(EventHandler)}
     * event point if set.  If no detached event handler is set the endpoint will route the detached event to the
     * closed event handler if set and allow it to process the event in one location.
     *
     * @param remoteDetachHandler
     *      The {@link EventHandler} to notify when this link is remotely closed.
     *
     * @return the {@link Link} for chaining.
     */
    L detachHandler(EventHandler<L> remoteDetachHandler);

    /**
     * Handler for link credit updates that occur after a remote {@link Flow} arrives.
     *
     * @param handler
     *      An event handler that will be signaled when the link credit is updated by a remote flow.
     *
     * @return the {@link Link} for chaining.
     */
    L creditStateUpdateHandler(EventHandler<L> handler);

}
