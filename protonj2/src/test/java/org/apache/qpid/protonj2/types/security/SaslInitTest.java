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
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.security.SaslInit;
import org.apache.qpid.protonj2.types.security.SaslPerformative.SaslPerformativeHandler;
import org.apache.qpid.protonj2.types.security.SaslPerformative.SaslPerformativeType;
import org.junit.Test;

public class SaslInitTest {

    @Test
    public void testInvoke() {
        AtomicReference<String> result = new AtomicReference<>();
        new SaslInit().invoke(new SaslPerformativeHandler<String>() {

            @Override
            public void handleInit(SaslInit saslInit, String context) {
                result.set(context);
            }
        }, "test");

        assertEquals("test", result.get());
    }

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new SaslInit().toString());
    }

    @Test
    public void testGetDataFromEmptySection() {
        assertNull(new SaslInit().getHostname());
        assertNull(new SaslInit().getInitialResponse());
        assertNull(new SaslInit().getMechanism());
    }

    @Test
    public void testCopyFromEmpty() {
        assertNull(new SaslInit().copy().getHostname());
    }

    @Test
    public void testMechanismRequired() {
        SaslInit init = new SaslInit();

        try {
            init.setMechanism(null);
            fail("Mechanism field is required and should not be cleared");
        } catch (NullPointerException npe) {}
    }

    @Test
    public void testCopy() {
        byte[] bytes = new byte[] { 1 };
        Binary binary = new Binary(bytes);

        SaslInit init = new SaslInit();

        init.setHostname("localhost");
        init.setInitialResponse(binary);
        init.setMechanism(Symbol.valueOf("ANONYMOUS"));

        SaslInit copy = init.copy();

        assertNotSame(copy, init);
        assertEquals(init.getHostname(), copy.getHostname());
        assertArrayEquals(init.getInitialResponse().getArray(), copy.getInitialResponse().getArray());
        assertEquals(init.getMechanism(), copy.getMechanism());
    }

    @Test
    public void testGetType() {
        assertEquals(SaslPerformativeType.INIT, new SaslInit().getPerformativeType());
    }
}
