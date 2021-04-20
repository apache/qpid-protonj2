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
package org.apache.qpid.protonj2.types.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.DeliveryState.DeliveryStateType;
import org.junit.jupiter.api.Test;

public class ModifiedTest {

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new Modified().toString());
    }

    @Test
    public void testCreateWithDeliveryFailed() {
        Modified modified = new Modified(true, false);
        assertTrue(modified.isDeliveryFailed());
        assertFalse(modified.isUndeliverableHere());
        assertNull(modified.getMessageAnnotations());
    }

    @Test
    public void testCreateWithDeliveryFailedAndUndeliverableHere() {
        Modified modified = new Modified(true, true);
        assertTrue(modified.isDeliveryFailed());
        assertTrue(modified.isUndeliverableHere());
        assertNull(modified.getMessageAnnotations());
    }

    @Test
    public void testCreateWithDeliveryFailedAndUndeliverableHereAndAnnotations() {
        Map<Symbol, Object> annotations = new HashMap<>();
        annotations.put(Symbol.valueOf("key1"), "value");
        annotations.put(Symbol.valueOf("key2"), "value");

        Modified modified = new Modified(true, true, annotations);
        assertTrue(modified.isDeliveryFailed());
        assertTrue(modified.isUndeliverableHere());
        assertNotNull(modified.getMessageAnnotations());

        assertEquals(annotations, modified.getMessageAnnotations());
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
