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
package org.apache.qpid.proton4j.types.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.transport.DeliveryState.DeliveryStateType;
import org.junit.Test;

public class ModifiedTest {

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new Modified().toString());
    }

    @Test
    public void testAnnotations() {
        Modified modified = new Modified();
        assertNull(modified.getMessageAnnotations());
        modified.setMessageAnnotations(new HashMap<Symbol, Object>());
        assertNotNull(modified.getMessageAnnotations());
    }

    @Test
    public void testDeliveryFailed() {
        Modified modified = new Modified();
        assertFalse(modified.isDeliveryFailed());
        modified.setDeliveryFailed(true);
        assertTrue(modified.isDeliveryFailed());
    }

    @Test
    public void testUndeliverableHere() {
        Modified modified = new Modified();
        assertFalse(modified.isUndeliverableHere());
        modified.setUndeliverableHere(true);
        assertTrue(modified.isUndeliverableHere());
    }

    @Test
    public void testGetAnnotationsFromEmptySection() {
        assertNull(new Modified().getMessageAnnotations());
    }

    @Test
    public void testGetType() {
        assertEquals(DeliveryStateType.Modified, new Modified().getType());
    }
}
