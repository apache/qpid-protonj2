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
package org.apache.qpid.proton4j.types.transactions;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.transactions.Coordinator;
import org.apache.qpid.proton4j.types.transactions.TxnCapability;
import org.junit.Test;

public class CoordinatorTest {

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new Coordinator().toString());
    }

    @Test
    public void testCopyOnEmpty() {
        assertNotNull(new Coordinator().copy());
    }

    @Test
    public void testCopy() {
        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);

        Coordinator copy = coordinator.copy();

        assertNotSame(copy.getCapabilities(), coordinator.getCapabilities());
        assertArrayEquals(copy.getCapabilities(), coordinator.getCapabilities());

        coordinator.setCapabilities(TxnCapability.LOCAL_TXN, TxnCapability.PROMOTABLE_TXN);

        copy = coordinator.copy();

        assertNotSame(copy.getCapabilities(), coordinator.getCapabilities());
        assertArrayEquals(copy.getCapabilities(), coordinator.getCapabilities());
    }

    @Test
    public void testCapabilities() {
        Coordinator coordinator = new Coordinator();

        assertNull(coordinator.getCapabilities());
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        assertNotNull(coordinator.getCapabilities());
        assertNotNull(coordinator.toString());

        assertArrayEquals(new Symbol[] { TxnCapability.LOCAL_TXN }, coordinator.getCapabilities());
    }

    @Test
    public void testToStringWithCapabilities() {
        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        assertNotNull(new Coordinator().toString());
    }
}
