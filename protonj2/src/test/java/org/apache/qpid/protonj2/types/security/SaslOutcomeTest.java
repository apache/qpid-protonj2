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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.security.SaslPerformative.SaslPerformativeHandler;
import org.apache.qpid.protonj2.types.security.SaslPerformative.SaslPerformativeType;
import org.junit.Test;

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
}
