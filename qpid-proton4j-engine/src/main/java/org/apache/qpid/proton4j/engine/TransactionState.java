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

import org.apache.qpid.proton4j.amqp.transactions.Declare;
import org.apache.qpid.proton4j.amqp.transactions.Discharge;

/**
 * Indicates the current state of a given {@link Transaction}
 */
public enum TransactionState {

    /**
     * A {@link Transaction} is considered IDLE until the {@link TransactionManager} responds that
     * it has been declared successfully and an transaction Id has been assigned.
     */
    IDLE,

    /**
     * A {@link Transaction} is considered declaring once a Declare command has been sent to the remote
     * but before any response has been received which assigns the transaction ID.
     */
    DECLARING,

    /**
     * A {@link Transaction} is considered declared once the {@link TransactionManager} has responded
     * in the affirmative and assigned a transaction Id.
     */
    DECLARED,

    /**
     * A {@link Transaction} is considered to b discharging once a Discharge command has been sent to the remote
     * but before any response has been received indicating the outcome of the attempted discharge.
     */
    DISCHARGING,

    /**
     * A {@link Transaction} is considered discharged once a {@link Discharge} has been requested and
     * the {@link TransactionManager} has responded in the affirmative that the request has been honored.
     */
    DISCHARGED,

    /**
     * A {@link Transaction} is considered failed in the {@link TransactionManager} responds with an error
     * to either the {@link Declare} action or the {@link Discharge} action.
     */
    FAILED;

}
