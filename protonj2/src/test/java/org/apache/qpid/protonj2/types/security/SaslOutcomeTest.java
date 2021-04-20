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
package org.apache.qpid.protonj2.types.security;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.security.SaslPerformative.SaslPerformativeHandler;
import org.apache.qpid.protonj2.types.security.SaslPerformative.SaslPerformativeType;
import org.junit.jupiter.api.Test;

public class SaslOutcomeTest {

    @Test
    public void testInvoke() {
        AtomicReference<String> result = new AtomicReference<>();
        new SaslOutcome().invoke(new SaslPerformativeHandler<String>() {

            @Override
            public void handleOutcome(SaslOutcome saslOutcome, String context) {
                result.set(context);
            }
        }, "test");

        assertEquals("test", result.get());
    }

    @Test
    public void testToStringOnNonEmptyObject() {
        byte[] bytes = new byte[] { 1 };
        Binary binary = new Binary(bytes);

        SaslOutcome value = new SaslOutcome();

        value.setCode(SaslCode.OK);
        value.setAdditionalData(binary);

        assertNotNull(value.toString());
    }

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new SaslOutcome().toString());
    }

    @Test
    public void testGetDataFromEmptySection() {
        assertNull(new SaslOutcome().getCode());
        assertNull(new SaslOutcome().getAdditionalData());
    }

    @Test
    public void testCopyFromEmpty() {
        assertNull(new SaslOutcome().copy().getCode());
    }

    @Test
    public void testMechanismRequired() {
        SaslOutcome init = new SaslOutcome();

        try {
            init.setCode(null);
            fail("Outcome field is required and should not be cleared");
        } catch (NullPointerException npe) {}
    }

    @Test
    public void testCopy() {
        byte[] bytes = new byte[] { 1 };
        Binary binary = new Binary(bytes);

        SaslOutcome value = new SaslOutcome();

        value.setCode(SaslCode.OK);
        value.setAdditionalData(binary);

        SaslOutcome copy = value.copy();

        assertNotSame(copy, value);
        assertEquals(copy.getCode(), value.getCode());
        assertArrayEquals(value.getAdditionalData().getArray(), copy.getAdditionalData().getArray());
    }

    @Test
    public void testGetType() {
        assertEquals(SaslPerformativeType.OUTCOME, new SaslOutcome().getPerformativeType());
    }

    @Test
    public void testPerformativeHandlerInvocations() {
        final SaslOutcome value = new SaslOutcome();
        final AtomicBoolean invoked = new AtomicBoolean();

        value.invoke(new SaslPerformativeHandler<AtomicBoolean>() {

            @Override
            public void handleOutcome(SaslOutcome saslOutcome, AtomicBoolean context) {
                context.set(true);
            }

        }, invoked);

        assertTrue(invoked.get());
    }
}
