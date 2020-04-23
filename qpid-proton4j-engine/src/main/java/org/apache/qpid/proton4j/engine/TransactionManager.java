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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.transactions.Declare;
import org.apache.qpid.proton4j.amqp.transactions.Discharge;

/**
 * Transaction Manager endpoint that implements the mechanics of handling the declaration
 * of and the requested discharge of AMQP transactions.  Typically an AMQP server  instance
 * will host the transaction management services that are used by client resources to declare
 * and discharge transaction and handle the associated of deliveries that are enlisted in
 * active transactions.
 */
public interface TransactionManager extends Endpoint<TransactionManager> {

    /**
     * Respond to a previous {@link Declare} request from the remote {@link TransactionController}
     * indicating that the requested transaction has been successfully declared and that deliveries
     * can now be enlisted in that transaction.
     *
     * @param transaction
     *      The transaction instance that is associated with the declared transaction.
     * @param txnId
     *      The binary transaction Id to assign the now declared transaction instance.
     *
     * @return this {@link TransactionManager}.
     */
    TransactionManager declared(Transaction<TransactionManager> transaction, Binary txnId);

    /**
     * Respond to a previous {@link Discharge} request from the remote {@link TransactionController}
     * indicating that the discharge completed on the transaction identified by given transaction Id
     * has now been retired.
     *
     * @param transaction
     *      The {@link Transaction} instance that has been discharged and is now retired.
     *
     * @return this {@link TransactionManager}.
     */
    TransactionManager discharged(Transaction<TransactionManager> transaction);

    /**
     * Called when the {@link TransactionController} end of the link has requested a new transaction be
     * declared using the information provided in the given {@link Declare} instance.
     *
     * @param declaredEventHandler
     *      handler that will act on the transaction declaration request.
     *
     * @return this {@link TransactionManager}.
     */
    TransactionManager declare(EventHandler<Transaction<TransactionManager>> declaredEventHandler);

    /**
     * Called when the {@link TransactionController} end of the link has requested a current transaction be
     * discharged using the information provided in the given {@link Discharge} instance.
     *
     * @param declaredEventHandler
     *      handler that will act on the transaction declaration request.
     *
     * @return this {@link TransactionManager}.
     */
    TransactionManager discharge(EventHandler<Transaction<TransactionManager>> declaredEventHandler);

}
