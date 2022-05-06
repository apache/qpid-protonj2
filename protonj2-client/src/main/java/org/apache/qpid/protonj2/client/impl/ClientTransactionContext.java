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
package org.apache.qpid.protonj2.client.impl;

import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.types.transactions.TransactionalState;
import org.apache.qpid.protonj2.types.transport.DeliveryState;

/**
 * Base for a Transaction Context used in {@link ClientSession} instances
 * to mask from the senders and receivers the work of deciding transaction
 * specific behaviors.
 */
public interface ClientTransactionContext {

    public interface Sendable {

        /**
         * Performs the actual send of delivery data which might be enlisted in a transaction
         * or may simply be a passed through based on the context and its state. The sender need
         * not be aware of this though as the context will provide a delivery state that is
         * appropriate for this send which would encapsulate any sender provided delivery state.
         *
         * @param state
         * 		Sender provided delivery state or context decorated version.
         * @param settled
         * 		If the send should be sent settled or not.
         */
        void send(DeliveryState state, boolean settled);

        /**
         * If the context that is overseeing this send is in a failed state it can request that
         * the send be discarded without notification to the sender that it failed, this occurs
         * most often in an in-doubt transaction context where all work will be dropped once the
         * user attempt to retire the transaction.
         */
        void discard();

    }

    /**
     * Begin a new transaction if one is not already in play.
     *
     * @param beginFuture
     *      The future that awaits the result of starting the new transaction.
     *
     * @return this {@link ClientTransactionContext} instance.
     *
     * @throws ClientIllegalStateException if an error occurs do to the transaction state.
     */
    ClientTransactionContext begin(ClientFuture<Session> beginFuture) throws ClientIllegalStateException;

    /**
     * Commits the current transaction if one is active and is not failed into a roll-back only
     * state.
     *
     * @param commitFuture
     *      The future that awaits the result of committing the new transaction.
     * @param startNew
     *      Should the context immediately initiate a new transaction
     *
     * @return this {@link ClientTransactionContext} instance.
     *
     * @throws ClientIllegalStateException if an error occurs do to the transaction state.
     */
    ClientTransactionContext commit(ClientFuture<Session> commitFuture, boolean startNew) throws ClientIllegalStateException;

    /**
     * Rolls back the current transaction if one is active.
     *
     * @param rollbackFuture
     *      The future that awaits the result of rolling back the new transaction.
     * @param startNew
     *      Should the context immediately initiate a new transaction
     *
     * @return this {@link ClientTransactionContext} instance.
     *
     * @throws ClientIllegalStateException if an error occurs do to the transaction state.
     */
    ClientTransactionContext rollback(ClientFuture<Session> rollbackFuture, boolean startNew) throws ClientIllegalStateException;

    /**
     * @return true if the context is hosting an active transaction.
     */
    boolean isInTransaction();

    /**
     * @return true if there is an active transaction but its state is failed an will roll-back
     */
    boolean isRollbackOnly();

    /**
     * Enlist the given outgoing envelope into this transaction if one is active and not already
     * in a roll-back only state.  If the transaction is failed the context should discard the
     * envelope which should appear to the caller as if the send was successful.
     *
     * @param sendable
     *      The envelope containing the details and mechanisms for sending the message.
     * @param state
     *      The delivery state that is being applied as the outcome of the delivery.
     * @param settled
     *      The settlement value that is being requested for the delivery.
     *
     * @return this {@link ClientTransactionContext} instance.
     */
    ClientTransactionContext send(Sendable sendable, DeliveryState state, boolean settled);

    /**
     * Apply a disposition to the given delivery wrapping it with a {@link TransactionalState} outcome
     * if there is an active transaction.  If there is no active transaction than the context will apply
     * the disposition as requested but if there is an active transaction then the disposition must be
     * wrapped in a {@link TransactionalState} and settlement should always enforced by the client.
     *
     * @param delivery
     *      The incoming delivery that the receiver is applying a disposition to.
     * @param state
     *      The delivery state that is being applied as the outcome of the delivery.
     * @param settled
     *      The settlement value that is being requested for the delivery.
     *
     * @return this {@link ClientTransactionContext} instance.
     */
    ClientTransactionContext disposition(IncomingDelivery delivery, DeliveryState state, boolean settled);

}
