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
package org.apache.qpid.protonj2.types.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class DetachTest {

    @Test
    public void testGetPerformativeType() {
        assertEquals(Performative.PerformativeType.DETACH, new Detach().getPerformativeType());
    }

    @Test
    public void testToStringOnFreshInstance() {
        assertNotNull(new Detach().toString());
    }

    @Test
    public void testDetachIsPresentChecks() {
        Detach detach = new Detach();

        assertTrue(detach.isEmpty());
        assertFalse(detach.hasClosed());
        assertFalse(detach.hasError());
        assertFalse(detach.hasHandle());

        detach.setClosed(false);
        detach.setHandle(1);
        detach.setError(new ErrorCondition("error", "error"));

        assertFalse(detach.isEmpty());
        assertTrue(detach.hasClosed());
        assertTrue(detach.hasError());
        assertTrue(detach.hasHandle());
    }

    @Test
    public void testSetHandleEnforcesLimits() {
        Detach detach = new Detach();

        assertThrows(IllegalArgumentException.class, () -> detach.setHandle(Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> detach.setHandle(-1l));
    }

    @Test
    public void testCopyFromNew() {
        Detach original = new Detach();
        Detach copy = original.copy();

        assertEquals(original.getClosed(), copy.getClosed());
        assertEquals(original.getError(), copy.getError());
    }

    @Test
    public void testCopyWithError() {
        Detach original = new Detach();
        original.setError(new ErrorCondition(AmqpError.DECODE_ERROR, "test"));

        Detach copy = original.copy();

        assertNotSame(copy.getError(), original.getError());
        assertEquals(original.getError(), copy.getError());
    }
}
