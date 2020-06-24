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

import java.util.Collection;

import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Terminus;
import org.apache.qpid.protonj2.types.transactions.Coordinator;
import org.apache.qpid.protonj2.types.transactions.Declare;
import org.apache.qpid.protonj2.types.transactions.Declared;
import org.apache.qpid.protonj2.types.transactions.Discharge;

/**
 * Transaction Controller link that implements the mechanics of declaring and discharging
 * AMQP transactions.  A {@link TransactionController} is typically used at the client side
 * of an AMQP {@link Link} to create {@link Transaction} instances which the client application
 * will enlist its incoming and outgoing deliveries into.
 */
public interface TransactionController extends Endpoint<TransactionController> {

    /**
     * Returns <code>true</code> if the {@link TransactionController} has capacity to send or buffer
     * and {@link Transaction} command to {@link Declare} or {@link Discharge}.  If no capacity then
     * a call to {@link TransactionController#declare()} or to
     * {@link TransactionController#discharge(Transaction, boolean)} would throw an exception.
     *
     * @return true if the controller will allow declaring or discharging a transaction at this time.
     */
    boolean hasCapacity();

    /**
     * Sets the {@link Source} to assign to the local end of this {@link TransactionController}.
     *
     * Must be called during setup, i.e. before calling the {@link #open()} method.
     *
     * @param source
     *      The {@link Source} that will be set on the local end of this transaction controller.
     *
     * @return this transaction controller instance.
     *
     * @throws IllegalStateException if the {@link TransactionController} has already been opened.
     */
    TransactionController setSource(Source source) throws IllegalStateException;

    /**
     * @return the {@link Source} for the local end of this {@link TransactionController}.
     */
    Source getSource();

    /**
     * Sets the {@link Coordinator} target to assign to the local end of this {@link TransactionController}.
     *
     * Must be called during setup, i.e. before calling the {@link #open()} method.
     *
     * @param coordinator
     *      The {@link Coordinator} target that will be set on the local end of this transaction controller.
     *
     * @return this transaction controller instance.
     *
     * @throws IllegalStateException if the {@link TransactionController} has already been opened.
     */
    TransactionController setCoordinator(Coordinator coordinator) throws IllegalStateException;

    /**
     * Returns the currently set Coordinator target for this {@link Link}.
     *
     * @return the link target {@link Coordinator} for the local end of this link.
     */
    Coordinator getCoordinator();

    /**
     * @return the source {@link Source} for the remote end of this {@link TransactionController}.
     */
    Source getRemoteSource();

    /**
     * Returns the remote target {@link Terminus} for this transaction controller which must be of type
     * {@link Coordinator} or null if remote did not set a terminus.
     *
     * @return the remote coordinator {@link Terminus} for the remote end of this link.
     */
    Coordinator getRemoteCoordinator();

    /**
     * Returns a list of {@link Transaction} objects that are active within this {@link TransactionController} which
     * have not reached a terminal state meaning they have not been successfully discharged and have not failed in
     * either the {@link Declare} phase or the {@link Discharge} phase.  If there are no transactions active within
     * this {@link TransactionController} this method returns an empty {@link Collection}.
     *
     * @return a list of Transactions that are allocated to this controller that have not reached a terminal state.
     */
    Collection<Transaction<TransactionController>> transactions();

    /**
     * Creates a new {@link Transaction} instances that is returned in the {@link TransactionState#IDLE} state
     * which can be populated with application specific attachments or assigned a linked resource prior to calling
     * the
     *
     * @return a new {@link Transaction} instance that can be correlated with later declared events.
     */
    Transaction<TransactionController> newTransaction();

    /**
     * Request that the remote {@link TransactionManager} declare a new transaction and
     * respond with a new transaction Id for that transaction.  Upon successful declaration of
     * a new transaction the remote will respond and the {@link TransactionController#declaredHandler(EventHandler)}
     * event handler will be signaled.
     *
     * This is a convenience method that is the same as first calling {@link TransactionController#newTransaction()}
     * and then passing the result of that to the {@link TransactionController#declare(Transaction)} method.
     *
     * @return a new {@link Transaction} instance that can be correlated with later declared events.
     */
    Transaction<TransactionController> declare();

    /**
     * Request that the remote {@link TransactionManager} declare a new transaction and
     * respond with a new transaction Id for that transaction.  Upon successful declaration of
     * a new transaction the remote will respond and the {@link TransactionController#declaredHandler(EventHandler)}
     * event handler will be signaled.
     *
     * @param transaction
     *      The {@link Transaction} that is will be associated with the eventual declared transaction.
     *
     * @return this {@link TransactionController}
     */
    TransactionController declare(Transaction<TransactionController> transaction);

    /**
     * Request that the remote {@link TransactionManager} discharge the given transaction and
     * with the specified failure state (true for failed).  Upon successful declaration of
     * a new transaction the remote will respond and the {@link TransactionController#declaredHandler(EventHandler)}
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
    TransactionController declaredHandler(EventHandler<Transaction<TransactionController>> declaredEventHandler);

    /**
     * Called when the {@link TransactionManager} end of the link responds to a {@link Transaction} declaration
     * with an {@link Rejected} outcome indicating that the transaction could not be successfully declared.
     *
     * @param declareFailureEventHandler
     *      An {@link EventHandler} that will be called when a previous transaction declaration fails.
     *
     * @return this {@link TransactionController}.
     */
    TransactionController declareFailureHandler(EventHandler<Transaction<TransactionController>> declareFailureEventHandler);

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
    TransactionController dischargedHandler(EventHandler<Transaction<TransactionController>> dischargedEventHandler);

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
    TransactionController dischargeFailureHandler(EventHandler<Transaction<TransactionController>> dischargeFailureEventHandler);

    /**
     * Allows the caller to register an {@link EventHandler} that will be signaled when the underlying
     * link for this {@link TransactionController} has been granted credit which would then allow for
     * transaction {@link Declared} and {@link Discharge} commands to be sent to the remote Transactional
     * Resource.
     *
     * If the controller already has credit to send then the handler will be invoked immediately otherwise
     * it will be stored until credit becomes available.  Once a handler is signaled it is no longer retained
     * for future updates and the caller will need to register it again once more transactional work is to be
     * completed.  Because more than one handler can be added at a time the caller should check again before
     * attempting to perform a transaction {@link Declared} or {@link Discharge} is performed as other tasks
     * might have already consumed credit if work is done via some asynchronous mechanism.
     *
     * @param handler
     *      The {@link EventHandler} that will be signaled once credit is available for transaction work.
     *
     * @return this {@link TransactionController} instance.
     */
    TransactionController registerCapacityAvailableHandler(EventHandler<TransactionController> handler);

}
