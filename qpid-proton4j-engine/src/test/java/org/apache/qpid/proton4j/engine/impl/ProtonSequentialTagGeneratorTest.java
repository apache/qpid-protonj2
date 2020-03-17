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
package org.apache.qpid.proton4j.engine.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import org.apache.qpid.proton4j.amqp.DeliveryTag;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.DeliveryTagGenerator;
import org.junit.Test;

public class ProtonSequentialTagGeneratorTest {

    @Test
    public void testCreateTagGenerator() {
        DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.SEQUENTIAL.createGenerator();
        assertTrue(generator instanceof ProtonSequentialTagGenerator);
    }

    @Test
    public void testCreateTag() {
        ProtonSequentialTagGenerator generator = new ProtonSequentialTagGenerator();

        assertNotNull(generator.nextTag());
    }

    @Test
    public void testCopyTag() {
        ProtonSequentialTagGenerator generator = new ProtonSequentialTagGenerator();
        DeliveryTag next = generator.nextTag();
        DeliveryTag copy = next.copy();

        assertNotSame(next, copy);
        assertEquals(next, copy);
    }

    @Test
    public void testTagEquals() {
        ProtonSequentialTagGenerator generator = new ProtonSequentialTagGenerator();

        DeliveryTag tag1 = generator.nextTag();
        DeliveryTag tag2 = generator.nextTag();
        DeliveryTag tag3 = generator.nextTag();

        assertEquals(tag1, tag1);
        assertNotEquals(tag1, tag2);
        assertNotEquals(tag2, tag3);
        assertNotEquals(tag1, tag3);

        assertNotEquals(null, tag1);
        assertNotEquals(tag1, null);
        assertNotEquals("something", tag1);
        assertNotEquals(tag2, "something");
    }

    @Test
    public void testCreateTagsThatAreEqual() {
        ProtonSequentialTagGenerator generator = new ProtonSequentialTagGenerator();

        generator.setNextTagId(42);
        DeliveryTag tag1 = generator.nextTag();

        generator.setNextTagId(42);
        DeliveryTag tag2 = generator.nextTag();

        assertNotSame(tag1, tag2);
        assertEquals(tag1, tag2);

        assertEquals(tag1.hashCode(), tag2.hashCode());
    }

    @Test
    public void testCreateTagsThatWrapAroundLimit() {
        ProtonSequentialTagGenerator generator = new ProtonSequentialTagGenerator();

        // Test that on wrap the tags start beyond the cached values.
        generator.setNextTagId(0xFFFFFFFFFFFFFFFFl);

        DeliveryTag maxUnsignedLong = generator.nextTag();
        DeliveryTag nextTagAfterWrap = generator.nextTag();

        assertEquals(Long.BYTES, maxUnsignedLong.tagBytes().length);
        assertEquals(Byte.BYTES, nextTagAfterWrap.tagBytes().length);
    }

    @Test
    public void testCreateMatchingValuesFromWrittenBuffer() {
        ProtonSequentialTagGenerator generator = new ProtonSequentialTagGenerator();
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(64);

        generator.setNextTagId(-127);                // Long
        generator.nextTag().writeTo(buffer);
        generator.setNextTagId(127);                 // Byte
        generator.nextTag().writeTo(buffer);
        generator.setNextTagId(256);                 // Short
        generator.nextTag().writeTo(buffer);
        generator.setNextTagId(65536);               // Int
        generator.nextTag().writeTo(buffer);
        generator.setNextTagId(0x00000001FFFFFFFFl); // Long
        generator.nextTag().writeTo(buffer);

        assertEquals(23, buffer.getReadableBytes());

        assertEquals(-127, buffer.readLong());
        assertEquals(127, buffer.readByte());
        assertEquals(256, buffer.readShort());
        assertEquals(65536, buffer.readInt());
        assertEquals(0x00000001FFFFFFFFl, buffer.readLong());
    }

    @Test
    public void testTagSizeMatchesValueRange() {
        ProtonSequentialTagGenerator generator = new ProtonSequentialTagGenerator();

        generator.setNextTagId(-127);
        assertEquals(Long.BYTES, generator.nextTag().tagLength());
        assertEquals(Long.BYTES, generator.nextTag().tagBytes().length);
        assertEquals(Long.BYTES, generator.nextTag().tagBuffer().getReadableBytes());

        generator.setNextTagId(127);
        assertEquals(Byte.BYTES, generator.nextTag().tagLength());
        assertEquals(Byte.BYTES, generator.nextTag().tagBytes().length);
        assertEquals(Byte.BYTES, generator.nextTag().tagBuffer().getReadableBytes());

        generator.setNextTagId(256);
        assertEquals(Short.BYTES, generator.nextTag().tagLength());
        assertEquals(Short.BYTES, generator.nextTag().tagBytes().length);
        assertEquals(Short.BYTES, generator.nextTag().tagBuffer().getReadableBytes());

        generator.setNextTagId(65536);
        assertEquals(Integer.BYTES, generator.nextTag().tagLength());
        assertEquals(Integer.BYTES, generator.nextTag().tagBytes().length);
        assertEquals(Integer.BYTES, generator.nextTag().tagBuffer().getReadableBytes());

        generator.setNextTagId(0x00000001FFFFFFFFl);
        assertEquals(Long.BYTES, generator.nextTag().tagLength());
        assertEquals(Long.BYTES, generator.nextTag().tagBytes().length);
        assertEquals(Long.BYTES, generator.nextTag().tagBuffer().getReadableBytes());
    }
}
