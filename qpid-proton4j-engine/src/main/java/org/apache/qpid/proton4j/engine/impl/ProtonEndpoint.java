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

import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.engine.Endpoint;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EventHandler;

/**
 * Proton abstract {@link Endpoint} implementation that provides some common facilities.
 *
 * @param <E>
 */
public abstract class ProtonEndpoint<E extends Endpoint<E>> implements Endpoint<E> {

    protected final ProtonEngine engine;

    private ProtonAttachments attachments;
    private Object linkedResource;

    private ErrorCondition localError;
    private ErrorCondition remoteError;

    private EventHandler<E> remoteOpenHandler;
    private EventHandler<E> remoteCloseHandler;
    private EventHandler<E> localOpenHandler;
    private EventHandler<E> localCloseHandler;
    private EventHandler<Engine> engineShutdownHandler;

    /**
     * Create a new {@link ProtonEndpoint} instance with the given Engine as the owner.
     *
     * @param engine
     *      The {@link Engine} that this {@link Endpoint} belongs to.
     */
    public ProtonEndpoint(ProtonEngine engine) {
        this.engine = engine;
    }

    @Override
    public void setLinkedResource(Object resource) {
        this.linkedResource = resource;
    }

    @Override
    public Object getLinkedResource() {
        return linkedResource;
    }

    @Override
    public <T> T getLinkedResource(Class<T> typeClass) {
        return typeClass.cast(linkedResource);
    }

    @Override
    public ProtonEngine getEngine() {
        return engine;
    }

    @Override
    public ProtonAttachments getAttachments() {
        return attachments == null ? attachments = new ProtonAttachments() : attachments;
    }

    @Override
    public ErrorCondition getCondition() {
        return localError;
    }

    @Override
    public E setCondition(ErrorCondition condition) {
        localError = condition == null ? null : condition.copy();
        return self();
    }

    @Override
    public ErrorCondition getRemoteCondition() {
        return remoteError;
    }

    E setRemoteCondition(ErrorCondition condition) {
        remoteError = condition == null ? null : condition.copy();
        return self();
    }

    //----- Abstract methods

    abstract E self();

    //----- Event handler registration and access

    @Override
    public E openHandler(EventHandler<E> remoteOpenEventHandler) {
        this.remoteOpenHandler = remoteOpenEventHandler;
        return self();
    }

    EventHandler<E> openHandler() {
        return remoteOpenHandler;
    }

    E fireRemoteOpen() {
        if (remoteOpenHandler != null) {
            remoteOpenHandler.handle(self());
        }

        return self();
    }

    @Override
    public E closeHandler(EventHandler<E> remoteCloseEventHandler) {
        this.remoteCloseHandler = remoteCloseEventHandler;
        return self();
    }

    EventHandler<E> closeHandler() {
        return remoteCloseHandler;
    }

    E fireRemoteClose() {
        if (remoteCloseHandler != null) {
            remoteCloseHandler.handle(self());
        }

        return self();
    }

    @Override
    public E localOpenHandler(EventHandler<E> localOpenEventHandler) {
        this.localOpenHandler = localOpenEventHandler;
        return self();
    }

    EventHandler<E> localOpenHandler() {
        return localOpenHandler;
    }

    E fireLocalOpen() {
        if (localOpenHandler != null) {
            localOpenHandler.handle(self());
        }

        return self();
    }

    @Override
    public E localCloseHandler(EventHandler<E> localCloseEventHandler) {
        this.localCloseHandler = localCloseEventHandler;
        return self();
    }

    EventHandler<E> localCloseHandler() {
        return localCloseHandler;
    }

    E fireLocalClose() {
        if (localCloseHandler != null) {
            localCloseHandler.handle(self());
        }

        return self();
    }

    @Override
    public E engineShutdownHandler(EventHandler<Engine> engineShutdownEventHandler) {
        this.engineShutdownHandler = engineShutdownEventHandler;
        return self();
    }

    EventHandler<Engine> engineShutdownHandler() {
        return engineShutdownHandler;
    }

    Engine fireEngineShutdown() {
        if (engineShutdownHandler != null) {
            engineShutdownHandler.handle(engine);
        }

        return engine;
    }
}
