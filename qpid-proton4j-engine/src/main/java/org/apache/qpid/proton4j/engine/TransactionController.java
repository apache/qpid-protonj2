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

import org.apache.qpid.proton4j.amqp.messaging.Rejected;
import org.apache.qpid.proton4j.amqp.transactions.Declare;

/**
 * Transaction Controller link that handles the mechanics of declaring and discharging
 * AMQP transactions.  A {@link TransactionController} is typically used at the client side
 * of an AMQP {@link Link} to create {@link Transaction} instances in order to enlist incoming
 * and or outgoing Deliveries into.
 */
public interface TransactionController extends Link<TransactionController> {

    /**
     * Request that the remote {@link TransactionManager} declare a new transaction and
     * respond with a new transaction Id for that transaction.  Upon successful declaration of
     * a new transaction the remote will respond and the {@link TransactionController#declared(EventHandler)}
     * event handler will be signaled.
     *
     * @return a new {@link Transaction} instance that can be correlated with later declared events.
     */
    Transaction<TransactionController> declare();

    /**
     * Request that the remote {@link TransactionManager} discharge the given transaction and
     * with the specified failure state (true for failed).  Upon successful declaration of
     * a new transaction the remote will respond and the {@link TransactionController#declared(EventHandler)}
     * event handler will be signaled.
     *
     * @param transaction
     *      The {@link Transaction} that is being discharged.
     * @param failed
     *      boolean value indicating the the discharge indicates the transaction failed (rolled back).
     *
     * @return this {@link TransactionController}
     */
    TransactionController discharge(Transaction<TransactionController> transaction, boolean failed);

    /**
     * Called when the {@link TransactionManager} end of the link has responded to a previous
     * {@link Declare} request and the transaction can now be used to enroll deliveries into the
     * active transaction.
     *
     * @param declaredEventHandler
     *      An {@link EventHandler} that will act on the transaction declaration request.
     *
     * @return this {@link TransactionController}.
     */
    TransactionController declared(EventHandler<Transaction<TransactionController>> declaredEventHandler);

    /**
     * Called when the {@link TransactionManager} end of the link responds to a {@link Transaction} declaration
     * with an {@link Rejected} outcome indicating that the transaction could not be successfully declared.
     *
     * @param declareFailureEventHandler
     *      An {@link EventHandler} that will be called when a previous transaction declaration fails.
     *
     * @return this {@link TransactionController}.
     */
    TransactionController declareFailure(EventHandler<Transaction<TransactionController>> declareFailureEventHandler);

    /**
     * Called when the {@link TransactionManager} end of the link has responded to a previous
     * {@link TransactionController#discharge(Transaction, boolean)} request and the transaction has
     * been retired.
     *
     * @param dischargedEventHandler
     *      An {@link EventHandler} that will act on the transaction discharge request.
     *
     * @return this {@link TransactionController}.
     */
    TransactionController discharged(EventHandler<Transaction<TransactionController>> dischargedEventHandler);

    /**
     * Called when the {@link TransactionManager} end of the link has responded to a previous
     * {@link TransactionController#discharge(Transaction, boolean)} request and the transaction discharge
     * failed for some reason.
     *
     * @param dischargeFailureEventHandler
     *      An {@link EventHandler} that will act on the transaction discharge failed event.
     *
     * @return this {@link TransactionController}.
     */
    TransactionController dischargeFailure(EventHandler<Transaction<TransactionController>> dischargeFailureEventHandler);

}
