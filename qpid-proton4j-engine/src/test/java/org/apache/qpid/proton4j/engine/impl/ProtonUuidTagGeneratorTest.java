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

import java.util.UUID;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.DeliveryTagGenerator;
import org.apache.qpid.proton4j.types.DeliveryTag;
import org.junit.Test;

public class ProtonUuidTagGeneratorTest {

    @Test
    public void testCreateTagGenerator() {
        DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.UUID.createGenerator();
        assertTrue(generator instanceof ProtonUuidTagGenerator);
    }

    @Test
    public void testCreateTag() {
        ProtonUuidTagGenerator generator = new ProtonUuidTagGenerator();
        assertNotNull(generator.nextTag());
        DeliveryTag next = generator.nextTag();
        next.release();
        assertNotSame(next, generator.nextTag());
    }

    @Test
    public void testCopyTag() {
        ProtonUuidTagGenerator generator = new ProtonUuidTagGenerator();
        DeliveryTag next = generator.nextTag();
        DeliveryTag copy = next.copy();

        assertNotSame(next, copy);
        assertEquals(next, copy);
    }

    @Test
    public void testTagCreatedHasExpectedUnderlying() {
        ProtonUuidTagGenerator generator = new ProtonUuidTagGenerator();

        DeliveryTag tag = generator.nextTag();

        assertEquals(16, tag.tagLength());

        UUID uuid = UUID.nameUUIDFromBytes(tag.tagBytes());

        assertNotNull(uuid);
        assertNotEquals(tag.hashCode(), uuid.hashCode());
    }

    @Test
    public void testTagCreatedHasExpectedUnderlyingBuffer() {
        ProtonUuidTagGenerator generator = new ProtonUuidTagGenerator();

        DeliveryTag tag = generator.nextTag();

        assertEquals(16, tag.tagLength());

        UUID uuid = UUID.nameUUIDFromBytes(tag.tagBuffer().getArray());

        assertNotNull(uuid);
        assertNotEquals(tag.hashCode(), uuid.hashCode());
    }

    @Test
    public void testCreateMatchingUUIDFromWrittenBuffer() {
        ProtonUuidTagGenerator generator = new ProtonUuidTagGenerator();
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(16, 16);

        DeliveryTag tag = generator.nextTag();

        tag.writeTo(buffer);

        assertEquals(16, buffer.getReadableBytes());

        UUID uuid = UUID.nameUUIDFromBytes(buffer.getArray());

        assertNotNull(uuid);
        assertNotEquals(tag.hashCode(), uuid.hashCode());
    }

    @Test
    public void testTagEquals() {
        ProtonUuidTagGenerator generator = new ProtonUuidTagGenerator();

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
    public void testCreateTagsAreNotEqual() {
        ProtonUuidTagGenerator generator = new ProtonUuidTagGenerator();

        DeliveryTag tag1 = generator.nextTag();
        DeliveryTag tag2 = generator.nextTag();

        assertNotSame(tag1, tag2);
        assertNotEquals(tag1, tag2);
    }
}
