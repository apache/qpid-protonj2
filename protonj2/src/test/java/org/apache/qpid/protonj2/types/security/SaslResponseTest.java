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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.security.SaslPerformative.SaslPerformativeHandler;
import org.apache.qpid.protonj2.types.security.SaslPerformative.SaslPerformativeType;
import org.junit.jupiter.api.Test;

public class SaslResponseTest {

    @Test
    public void testInvoke() {
        AtomicReference<String> result = new AtomicReference<>();
        new SaslResponse().invoke(new SaslPerformativeHandler<String>() {

            @Override
            public void handleResponse(SaslResponse saslResponse, String context) {
                result.set(context);
            }
        }, "test");

        assertEquals("test", result.get());
    }

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new SaslResponse().toString());
    }

    @Test
    public void testGetDataFromEmptySection() {
        assertNull(new SaslResponse().getResponse());
    }

    @Test
    public void testCopyFromEmpty() {
        assertNull(new SaslResponse().copy().getResponse());
    }

    @Test
    public void testMechanismRequired() {
        SaslResponse init = new SaslResponse();

        try {
            init.setResponse((Binary) null);
            fail("Response field is required and should not be cleared");
        } catch (NullPointerException npe) {}

        try {
            init.setResponse((ProtonBuffer) null);
            fail("Response field is required and should not be cleared");
        } catch (NullPointerException npe) {}
    }

    @Test
    public void testCopy() {
        byte[] bytes = new byte[] { 1 };
        Binary binary = new Binary(bytes);

        SaslResponse value = new SaslResponse();

        value.setResponse(binary);

        SaslResponse copy = value.copy();

        assertNotSame(copy, value);
        assertArrayEquals(value.getResponse().getArray(), copy.getResponse().getArray());
    }

    @Test
    public void testGetType() {
        assertEquals(SaslPerformativeType.RESPONSE, new SaslResponse().getPerformativeType());
    }
}
