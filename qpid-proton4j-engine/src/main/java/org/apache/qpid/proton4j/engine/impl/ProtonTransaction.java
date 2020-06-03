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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.engine.Endpoint;
import org.apache.qpid.proton4j.engine.Transaction;
import org.apache.qpid.proton4j.engine.TransactionController;
import org.apache.qpid.proton4j.engine.TransactionManager;
import org.apache.qpid.proton4j.engine.TransactionState;

/**
 * Base {@link Transaction} implementation that provides the basic functionality needed
 * to manage the {@link Transaction} that it represents.
 *
 * @param <E> The parent type for this {@link Transaction}
 */
public abstract class ProtonTransaction<E extends Endpoint<?>> implements Transaction<E> {

    private TransactionState state = TransactionState.IDLE;
    private ErrorCondition condition;
    private Binary txnId;

    private ProtonAttachments attachments;
    private Object linkedResource;

    @Override
    public TransactionState getState() {
        return state;
    }

    ProtonTransaction<E> setState(TransactionState state) {
        this.state = state;
        return this;
    }

    @Override
    public boolean isDeclared() {
        return state.ordinal() == TransactionState.DECLARED.ordinal();
    }

    @Override
    public boolean isDischarged() {
        return state.ordinal() == TransactionState.DISCHARGED.ordinal();
    }

    @Override
    public boolean isFailed() {
        return state.ordinal() == TransactionState.FAILED.ordinal();
    }

    @Override
    public ErrorCondition getCondition() {
        return condition;
    }

    ProtonTransaction<E> setCondition(ErrorCondition condition) {
        this.condition = condition;
        return this;
    }

    @Override
    public Binary getTxnId() {
        return txnId;
    }

    ProtonTransaction<E> getTxnId(Binary txnId) {
        this.txnId = txnId;
        return this;
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
    public ProtonAttachments getAttachments() {
        return attachments == null ? attachments = new ProtonAttachments() : attachments;
    }

    /**
     * Overridden by the {@link TransactionController} or {@link TransactionManager} that creates
     * this {@link Transaction} instance, this method returns the parent instance that created it.
     *
     * @return the resource that created this {@link Transaction}.
     */
    @Override
    public abstract E parent();

}
