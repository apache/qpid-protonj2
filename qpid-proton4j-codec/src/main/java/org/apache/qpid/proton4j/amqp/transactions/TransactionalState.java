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
package org.apache.qpid.proton4j.amqp.transactions;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Outcome;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;

public final class TransactionalState implements DeliveryState {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000034L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:transactional-state:list");

    private Binary txnId;
    private Outcome outcome;

    public Binary getTxnId() {
        return txnId;
    }

    public void setTxnId(Binary txnId) {
        if (txnId == null) {
            throw new NullPointerException("the txn-id field is mandatory");
        }

        this.txnId = txnId;
    }

    public Outcome getOutcome() {
        return outcome;
    }

    public void setOutcome(Outcome outcome) {
        this.outcome = outcome;
    }

    @Override
    public String toString() {
        return "TransactionalState{" + "txnId=" + txnId + ", outcome=" + outcome + '}';
    }

    @Override
    public DeliveryStateType getType() {
        return DeliveryStateType.Transactional;
    }
}
