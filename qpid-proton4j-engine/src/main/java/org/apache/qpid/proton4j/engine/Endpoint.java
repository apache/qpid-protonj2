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

import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.transport.ErrorCondition;

/**
 * Represents a conceptual endpoint type used to provide common operations that
 * all the endpoint types will share.
 *
 * @param <E> The {@link Endpoint} type
 */
public interface Endpoint<E extends Endpoint<E>> {

    /**
     * Open the end point locally, sending the Open performative immediately if possible or holding
     * it until SASL negotiations or the AMQP header exchange and other required performative exchanges
     * has completed.
     *
     * The end point will signal any registered handler of the remote opening the Connection
     * once the remote performative that signals open completion arrives.
     *
     * @return this {@link Endpoint} instance.
     *
     * @throws EngineStateException if an error occurs opening the Connection or the Engine is shutdown.
     */
    E open() throws EngineStateException;

    /**
     * Close the end point locally and send the closing performative immediately if possible or
     * holds it until the Connection / Engine state allows it.  If the engine encounters an error writing
     * the performative or the engine is in a failed state from a previous error then this method will
     * throw an exception.  If the engine has been shutdown then this method will close out the local
     * end of the {@link Endpoint} and clean up any local resources before returning normally.
     *
     * @return this {@link Endpoint} instance.
     *
     * @throws EngineFailedException if an error occurs closing the end point or the Engine is in a failed state.
     */
    E close() throws EngineFailedException;

    /**
     * @return the {@link Attachments} instance that is associated with this {@link Endpoint}
     */
    Attachments getAttachments();

    /**
     * @return the {@link Engine} which created this {@link Endpoint} instance.
     */
    Engine getEngine();

    /**
     * Gets the parent of this {@link Endpoint} which can be itself for {@link Connection} instance.
     *
     * @return the parent of this {@link Endpoint} or itself if this is a {@link Connection};
     */
    Endpoint<?> getParent();

    /**
     * Links a given resource to this {@link Endpoint}.
     *
     * @param resource
     *      The resource to link to this {@link Endpoint}.
     *
     * @return this {@link Endpoint} instance.
     */
    E setLinkedResource(Object resource);

    /**
     * @return the user set linked resource for this {@link Endpoint} instance.
     */
    <T> T getLinkedResource();

    /**
     * Gets the linked resource (if set) and returns it using the type information
     * provided to cast the returned value.
     *
     * @param <T> The type to cast the linked resource to if one is set.
     * @param typeClass the type's Class which is used for casting the returned value.
     *
     * @return the user set linked resource for this Context instance.
     *
     * @throws ClassCastException if the linked resource cannot be cast to the type requested.
     */
    <T> T getLinkedResource(Class<T> typeClass);

    //----- Operations on local end of this End Point

    /**
     * @return the local {@link Endpoint} error, or null if there is none
     */
    ErrorCondition getCondition();

    /**
     * Sets the local {@link ErrorCondition} to be applied to a {@link Endpoint} close.
     *
     * @param condition
     *      The error condition to convey to the remote peer on close of this end point.
     *
     * @return this {@link Endpoint} instance.
     */
    E setCondition(ErrorCondition condition);

    /**
     * Returns true if this {@link Endpoint} is currently locally open meaning that the {@link Endpoint#open()}
     * has been called but the {@link Endpoint#close()} has not.
     *
     * @return <code>true</code> if the {@link Endpoint} is locally open.
     *
     * @see Endpoint#isLocallyClosed()
     */
    boolean isLocallyOpen();

    /**
     * Returns true if this {@link Endpoint} is currently locally closed meaning that a call to the
     * {@link Endpoint#close} method has occurred.
     *
     * @return <code>true</code> if the {@link Endpoint} is locally closed.
     *
     * @see Endpoint#isLocallyOpen()
     */
    boolean isLocallyClosed();

    /**
     * Sets the capabilities to be offered on to the remote when this {@link Endpoint} is
     * opened.
     *
     * The offered capabilities value can only be modified prior to a call to {@link Endpoint#open()},
     * once the {@link Endpoint} has been opened locally an error will be thrown if this method
     * is called.
     *
     * @param capabilities
     *      The capabilities to be offered to the remote when the {@link Endpoint} is opened.
     *
     * @return this {@link Endpoint} instance.
     *
     * @throws IllegalStateException if the {@link Endpoint} has already been opened.
     */
    E setOfferedCapabilities(Symbol... capabilities) throws IllegalStateException;

    /**
     * @return the configured capabilities that are offered to the remote when the {@link Endpoint} is opened.
     */
    Symbol[] getOfferedCapabilities();

    /**
     * Sets the capabilities that are desired from the remote when this {@link Endpoint} is
     * opened.
     *
     * The desired capabilities value can only be modified prior to a call to {@link Endpoint#open()},
     * once the {@link Endpoint} has been opened locally an error will be thrown if this method
     * is called.
     *
     * @param capabilities
     *      The capabilities desired from the remote when the {@link Endpoint} is opened.
     *
     * @return this {@link Endpoint} instance.
     *
     * @throws IllegalStateException if the {@link Endpoint} has already been opened.
     */
    E setDesiredCapabilities(Symbol... capabilities) throws IllegalStateException;

    /**
     * @return the configured desired capabilities that are sent to the remote when the Connection is opened.
     */
    Symbol[] getDesiredCapabilities();

    /**
     * Sets the properties to be sent to the remote when this {@link Endpoint} is Opened.
     *
     * The {@link Endpoint} properties value can only be modified prior to a call to {@link Endpoint#open()},
     * once the {@link Endpoint} has been opened locally an error will be thrown if this method
     * is called.
     *
     * @param properties
     *      The properties that will be sent to the remote when this Connection is opened.
     *
     * @return this {@link Endpoint} instance.
     *
     * @throws IllegalStateException if the {@link Endpoint} has already been opened.
     */
    E setProperties(Map<Symbol, Object> properties) throws IllegalStateException;

    /**
     * @return the configured properties sent to the remote when this Connection is opened.
     */
    Map<Symbol, Object> getProperties();

    //----- Operations on remote end of this End Point

    /**
     * Returns true if this {@link Endpoint} is currently remotely open meaning that the AMQP performative
     * that completes the open phase of this {@link Endpoint}'s lifetime has arrived but the performative
     * that closes it has not.
     *
     * @return <code>true</code> if the {@link Endpoint} is remotely open.
     *
     * @see Endpoint#isRemotelyClosed()
     */
    boolean isRemotelyOpen();

    /**
     * Returns true if this {@link Endpoint} is currently remotely closed meaning that the AMQP performative
     * that completes the close phase of this {@link Endpoint}'s lifetime has arrived.
     *
     * @return <code>true</code> if the {@link Endpoint} is remotely closed.
     *
     * @see Endpoint#isRemotelyOpen()
     */
    boolean isRemotelyClosed();

    /**
     * If the remote has closed this {@link Endpoint} and provided an {@link ErrorCondition} as part
     * of the closing AMQP performative then this method will return it.
     *
     * @return the remote supplied {@link ErrorCondition}, or null if there is none.
     */
    ErrorCondition getRemoteCondition();

    /**
     * @return the capabilities offered by the remote when it opened its end of the {@link Endpoint}.
     */
    Symbol[] getRemoteOfferedCapabilities();

    /**
     * @return the capabilities desired by the remote when it opened its end of the {@link Endpoint}.
     */
    Symbol[] getRemoteDesiredCapabilities();

    /**
     * @return the properties sent by the remote when it opened its end of the {@link Endpoint}.
     */
    Map<Symbol, Object> getRemoteProperties();

    //----- Events for AMQP Endpoint resources

    /**
     * Sets a {@link EventHandler} for when an this {@link Endpoint} is opened locally via a call to {@link Endpoint#open()}
     *
     * Typically used by clients for logging or other state update event processing.  Clients should not perform any
     * blocking calls within this context.  It is an error for the handler to throw an exception and the outcome of
     * doing so is undefined.
     *
     * @param localOpenHandler
     *      The {@link EventHandler} to notify when this {@link Endpoint} is locally opened.
     *
     * @return this {@link Endpoint} instance.
     */
    E localOpenHandler(EventHandler<E> localOpenHandler);

    /**
     * Sets a {@link EventHandler} for when an this {@link Endpoint} is closed locally via a call to {@link Connection#close()}
     *
     * Typically used by clients for logging or other state update event processing.  Clients should not perform any
     * blocking calls within this context.  It is an error for the handler to throw an exception and the outcome of
     * doing so is undefined.
     *
     * @param localCloseHandler
     *      The {@link EventHandler} to notify when this {@link Endpoint} is locally closed.
     *
     * @return this {@link Endpoint} instance.
     */
    E localCloseHandler(EventHandler<E> localCloseHandler);

    /**
     * Sets a EventHandler for when an AMQP Open frame is received from the remote peer.
     *
     * Used to process remotely initiated Connections. Locally initiated sessions have their own EventHandler
     * invoked instead.  This method is typically used by servers to listen for the remote peer to open its
     * {@link Endpoint}, while a client would listen for the server to open its end of the {@link Endpoint} once
     * a local open has been performed.
     *
     * Typically used by clients as servers will typically listen to some parent resource event handler
     * to determine if the remote is initiating a resource open.
     *
     * @param remoteOpenEventHandler
     *          the EventHandler that will be signaled when the {@link Endpoint} has been remotely opened
     *
     * @return this {@link Endpoint} instance.
     */
    E openHandler(EventHandler<E> remoteOpenEventHandler);

    /**
     * Sets a EventHandler for when an AMQP Close frame is received from the remote peer.
     *
     * @param remoteCloseEventHandler
     *          the EventHandler that will be signaled when the {@link Endpoint} is remotely closed.
     *
     * @return this {@link Endpoint} instance.
     */
    E closeHandler(EventHandler<E> remoteCloseEventHandler);

    /**
     * Sets an {@link EventHandler} that is invoked when the engine that supports this {@link Endpoint} is shutdown
     * via a call to {@link Engine#shutdown()} which indicates a desire to terminate all engine operations. Any
     * {@link Endpoint} that has been both locally and remotely closed will not receive this event as it will no longer
     * be tracked by the parent its parent {@link Endpoint}.
     *
     * A typical use of this event would be from a locally closed {@link Endpoint} that is awaiting response from
     * the remote.  If this event fires then there will never be a remote response to any pending operations and
     * the client or server instance should react accordingly to clean up any related resources etc.
     *
     * @param engineShutdownEventHandler
     *      the EventHandler that will be signaled when this {@link Endpoint}'s engine is explicitly shutdown.
     *
     * @return this {@link Endpoint} instance.
     */
    E engineShutdownHandler(EventHandler<Engine> engineShutdownEventHandler);

}
