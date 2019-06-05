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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState.DeliveryStateType;
import org.junit.Test;

public class TransactionalStateTest {

    @Test
    public void testToString() {
        assertNotNull(new TransactionalState().toString());
    }

    @Test
    public void testTxnId() {
        Binary txnId = new Binary(new byte[] { 1 });
        TransactionalState state = new TransactionalState();

        assertNull(state.getTxnId());
        state.setTxnId(txnId);
        assertNotNull(state.getTxnId());

        try {
            state.setTxnId(null);
            fail("The TXN field is mandatory and cannot be set to null");
        } catch (NullPointerException npe) {}
    }

    @Test
    public void testOutcome() {
        TransactionalState state = new TransactionalState();

        assertNull(state.getOutcome());
        state.setOutcome(Accepted.getInstance());
        assertNotNull(state.getOutcome());
    }

    @Test
    public void testGetType() {
        assertEquals(DeliveryStateType.Transactional, new TransactionalState().getType());
    }
}
