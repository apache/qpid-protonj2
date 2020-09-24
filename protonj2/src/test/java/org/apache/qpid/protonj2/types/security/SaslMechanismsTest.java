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
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.security.SaslPerformative.SaslPerformativeHandler;
import org.apache.qpid.protonj2.types.security.SaslPerformative.SaslPerformativeType;
import org.junit.jupiter.api.Test;

public class SaslMechanismsTest {

    @Test
    public void testInvoke() {
        AtomicReference<String> result = new AtomicReference<>();
        new SaslMechanisms().invoke(new SaslPerformativeHandler<String>() {

            @Override
            public void handleMechanisms(SaslMechanisms saslMechanisms, String context) {
                result.set(context);
            }
        }, "test");

        assertEquals("test", result.get());
    }

    @Test
    public void testToStringOnNonEmptyObject() {
        Symbol[] mechanisms = new Symbol[] { Symbol.valueOf("EXTERNAL"), Symbol.valueOf("PLAIN") };
        SaslMechanisms value = new SaslMechanisms();

        value.setSaslServerMechanisms(mechanisms);

        assertNotNull(value.toString());
    }

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new SaslMechanisms().toString());
    }

    @Test
    public void testGetDataFromEmptySection() {
        assertNull(new SaslMechanisms().getSaslServerMechanisms());
    }

    @Test
    public void testCopyFromEmpty() {
        assertNull(new SaslMechanisms().copy().getSaslServerMechanisms());
    }

    @Test
    public void testMechanismRequired() {
        SaslMechanisms init = new SaslMechanisms();

        try {
            init.setSaslServerMechanisms((Symbol[]) null);
            fail("Server Mechanisms field is required and should not be cleared");
        } catch (NullPointerException npe) {}
    }

    @Test
    public void testCopy() {
        Symbol[] mechanisms = new Symbol[] { Symbol.valueOf("EXTERNAL"), Symbol.valueOf("PLAIN") };
        SaslMechanisms value = new SaslMechanisms();

        value.setSaslServerMechanisms(mechanisms);

        SaslMechanisms copy = value.copy();

        assertNotSame(copy, value);
        assertArrayEquals(value.getSaslServerMechanisms(), copy.getSaslServerMechanisms());
    }

    @Test
    public void testGetType() {
        assertEquals(SaslPerformativeType.MECHANISMS, new SaslMechanisms().getPerformativeType());
    }
}
