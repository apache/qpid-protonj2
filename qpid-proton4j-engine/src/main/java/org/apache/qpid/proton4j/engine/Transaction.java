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
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;

/**
 * A Transaction object that hold information and context for a single {@link Transaction}.
 *
 * @param <E> The parent of this Transaction either a {@link TransactionController} or {@link TransactionManager}
 */
public interface Transaction<E extends Link<?>> {

    /**
     * @return the current {@link Transaction} state.
     */
    TransactionState getState();

    /**
     * @return true if the {@link Transaction} has been marked declared by the {@link TransactionManager}.
     */
    boolean isDeclared();

    /**
     * @return true if the {@link Transaction} has been marked discharged by the {@link TransactionManager}.
     */
    boolean isDischareged();

    /**
     * The parent resource will mark the {@link Transaction} as failed is any of the operations performed on
     * it cannot be successfully completed such as a {@link Declare} operation failing to write due to an IO
     * error.
     *
     * @return true if the {@link Transaction} has been marked failed by the parent resource.
     */
    boolean isFailed();

    /**
     * If the declare or discharge of the transaction caused its state to become {@link TransactionState#FAILED}
     * this method returns the {@link ErrorCondition} that the remote used to describe the reason for the failure.
     *
     * @return the {@link ErrorCondition} that the {@link TransactionManager} used to fail the {@link Transaction}.
     */
    ErrorCondition getCondition();

    /**
     * Returns a reference to the parent of this {@link Transaction} which will be either a
     * {@link TransactionController} or a {@link TransactionManager} manager depending on the
     * end of the {@link Link} that is operating on the {@link Transaction}.
     *
     * @return a reference to the parent of this {@link Transaction}.
     */
    E parent();

    /**
     * Returns the transaction Id that is associated with the declared transaction.  Prior to a
     * {@link TransactionManager} completing a transaction declaration this method will return
     * null to indicate that the transaction has not been declared yet.
     *
     * @return the transaction Id associated with the transaction once successfully declared.
     */
    Binary getTxnId();

    /**
     * @return the {@link Context} instance that is associated with this {@link Transaction}
     */
    Context getContext();

}
