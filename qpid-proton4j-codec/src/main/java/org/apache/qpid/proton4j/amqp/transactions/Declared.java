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

public final class Declared implements DeliveryState, Outcome {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000033L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:declared:list");

    private Binary txnId;

    public Binary getTxnId() {
        return txnId;
    }

    public Declared setTxnId(Binary txnId) {
        if (txnId == null) {
            throw new NullPointerException("the txn-id field is mandatory");
        }

        this.txnId = txnId;
        return this;
    }

    @Override
    public String toString() {
        return "Declared{" + "txnId=" + txnId + '}';
    }

    @Override
    public DeliveryStateType getType() {
        return DeliveryStateType.Declared;
    }
}
