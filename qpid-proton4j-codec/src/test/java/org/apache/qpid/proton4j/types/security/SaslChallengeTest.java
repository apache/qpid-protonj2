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
package org.apache.qpid.proton4j.types.security;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.types.Binary;
import org.apache.qpid.proton4j.types.security.SaslPerformative.SaslPerformativeHandler;
import org.apache.qpid.proton4j.types.security.SaslPerformative.SaslPerformativeType;
import org.junit.Test;

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
}
