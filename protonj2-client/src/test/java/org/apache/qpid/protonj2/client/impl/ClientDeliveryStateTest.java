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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Transactional;
import org.apache.qpid.protonj2.client.impl.ClientDeliveryState.ClientTransactional;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Outcome;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.transactions.TransactionalState;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(20)
public class ClientDeliveryStateTest {

    @Test
    public void testAccepted() {
        final DeliveryState state = DeliveryState.accepted();

        assertNotNull(state);
        assertEquals(state.getType(), DeliveryState.Type.ACCEPTED);
        assertTrue(state.isAccepted());
        assertFalse(state.isRejected());
        assertFalse(state.isReleased());
        assertFalse(state.isModified());
        assertFalse(state.isTransactional());
    }

    @Test
    public void testRejected() {
        final DeliveryState state = DeliveryState.rejected("Error", "Problem");

        assertNotNull(state);
        assertEquals(state.getType(), DeliveryState.Type.REJECTED);
        assertFalse(state.isAccepted());
        assertTrue(state.isRejected());
        assertFalse(state.isReleased());
        assertFalse(state.isModified());
        assertFalse(state.isTransactional());
    }

    @Test
    public void testReleased() {
        final DeliveryState state = DeliveryState.released();

        assertNotNull(state);
        assertEquals(state.getType(), DeliveryState.Type.RELEASED);
        assertFalse(state.isAccepted());
        assertFalse(state.isRejected());
        assertTrue(state.isReleased());
        assertFalse(state.isModified());
        assertFalse(state.isTransactional());
    }

    @Test
    public void testModified() {
        final DeliveryState state = DeliveryState.modified(true, false);

        assertNotNull(state);
        assertEquals(state.getType(), DeliveryState.Type.MODIFIED);
        assertFalse(state.isAccepted());
        assertFalse(state.isRejected());
        assertFalse(state.isReleased());
        assertTrue(state.isModified());
        assertFalse(state.isTransactional());
    }

    @Test
    public void testTransactionalWithAccepted() {
        final Transactional state = new ClientTransactional(createTransactional(Accepted.getInstance()));

        assertNotNull(state);
        assertEquals(state.getType(), DeliveryState.Type.TRANSACTIONAL);
        assertTrue(state.isAccepted());
        assertFalse(state.isRejected());
        assertFalse(state.isReleased());
        assertFalse(state.isModified());
        assertTrue(state.isTransactional());
        assertTrue(state.getOutcome() instanceof org.apache.qpid.protonj2.client.Accepted);
    }

    @Test
    public void testTransactionalWithReleased() {
        final Transactional state = new ClientTransactional(createTransactional(Released.getInstance()));

        assertNotNull(state);
        assertEquals(state.getType(), DeliveryState.Type.TRANSACTIONAL);
        assertFalse(state.isAccepted());
        assertFalse(state.isRejected());
        assertTrue(state.isReleased());
        assertFalse(state.isModified());
        assertTrue(state.isTransactional());
        assertTrue(state.getOutcome() instanceof org.apache.qpid.protonj2.client.Released);
    }

    @Test
    public void testTransactionalWithModified() {
        final Map<Symbol, Object> symbolicAnnotations = Map.of(Symbol.valueOf("test"), "test");
        final Map<String, Object> annotations = Map.of("test", "test");

        final Transactional state = new ClientTransactional(createTransactional(new Modified(true, true, symbolicAnnotations)));

        assertNotNull(state);
        assertEquals(state.getType(), DeliveryState.Type.TRANSACTIONAL);
        assertFalse(state.isAccepted());
        assertFalse(state.isRejected());
        assertFalse(state.isReleased());
        assertTrue(state.isModified());
        assertTrue(state.isTransactional());
        assertTrue(state.getOutcome() instanceof org.apache.qpid.protonj2.client.Modified);

        org.apache.qpid.protonj2.client.Modified modifiedState = (org.apache.qpid.protonj2.client.Modified) state.getOutcome();

        assertNotNull(modifiedState);
        assertTrue(modifiedState.isDeliveryFailed());
        assertTrue(modifiedState.isUndeliverableHere());
        assertEquals(annotations, modifiedState.getMessageAnnotations());
    }

    @Test
    public void testTransactionalWithRejected() {
        final Map<Symbol, Object> symvolicInfo = Map.of(Symbol.valueOf("test"), "test");
        final Map<String, Object> info = Map.of("test", "test");

        final Transactional state = new ClientTransactional(createTransactional(new Rejected(new ErrorCondition("test", "data", symvolicInfo))));

        assertNotNull(state);
        assertEquals(state.getType(), DeliveryState.Type.TRANSACTIONAL);
        assertFalse(state.isAccepted());
        assertTrue(state.isRejected());
        assertFalse(state.isReleased());
        assertFalse(state.isModified());
        assertTrue(state.isTransactional());
        assertTrue(state.getOutcome() instanceof org.apache.qpid.protonj2.client.Rejected);

        org.apache.qpid.protonj2.client.Rejected rejectedState = (org.apache.qpid.protonj2.client.Rejected) state.getOutcome();

        assertNotNull(rejectedState);
        assertEquals("test", rejectedState.getCondition());
        assertEquals("data", rejectedState.getDescription());
        assertEquals(info, rejectedState.getInfo());
    }

    private TransactionalState createTransactional(Outcome outcome) {
        final TransactionalState txnState = new TransactionalState();

        txnState.setTxnId(new Binary(new byte[] { 0, 1, 2, 3}));
        txnState.setOutcome(outcome);

        return txnState;
    }
}
