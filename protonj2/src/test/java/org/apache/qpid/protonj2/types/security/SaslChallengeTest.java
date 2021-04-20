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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.security.SaslPerformative.SaslPerformativeHandler;
import org.apache.qpid.protonj2.types.security.SaslPerformative.SaslPerformativeType;
import org.junit.jupiter.api.Test;

public class SaslChallengeTest {

    @Test
    public void testInvoke() {
        AtomicReference<String> result = new AtomicReference<>();
        new SaslChallenge().invoke(new SaslPerformativeHandler<String>() {

            @Override
            public void handleChallenge(SaslChallenge saslChallenge, String context) {
                result.set(context);
            }
        }, "test");

        assertEquals("test", result.get());
    }

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new SaslChallenge().toString());
    }

    @Test
    public void testGetDataFromEmptySection() {
        assertNull(new SaslChallenge().getChallenge());
    }

    @Test
    public void testCopyFromEmpty() {
        assertNull(new SaslChallenge().copy().getChallenge());
    }

    @Test
    public void testMechanismRequired() {
        SaslChallenge init = new SaslChallenge();

        try {
            init.setChallenge((Binary) null);
            fail("Challenge field is required and should not be cleared");
        } catch (NullPointerException npe) {}

        try {
            init.setChallenge((ProtonBuffer) null);
            fail("Challenge field is required and should not be cleared");
        } catch (NullPointerException npe) {}
    }

    @Test
    public void testCopy() {
        byte[] bytes = new byte[] { 1 };
        Binary binary = new Binary(bytes);

        SaslChallenge value = new SaslChallenge();

        value.setChallenge(binary);

        SaslChallenge copy = value.copy();

        assertNotSame(copy, value);
        assertArrayEquals(value.getChallenge().getArray(), copy.getChallenge().getArray());
    }

    @Test
    public void testGetType() {
        assertEquals(SaslPerformativeType.CHALLENGE, new SaslChallenge().getPerformativeType());
    }

    @Test
    public void testPerformativeHandlerInvocations() {
        final SaslChallenge value = new SaslChallenge();
        final AtomicBoolean invoked = new AtomicBoolean();

        value.invoke(new SaslPerformativeHandler<AtomicBoolean>() {

            @Override
            public void handleChallenge(SaslChallenge saslChallenge, AtomicBoolean context) {
                context.set(true);
            }

        }, invoked);

        assertTrue(invoked.get());
    }
}
