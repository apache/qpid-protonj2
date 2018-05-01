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
package org.apache.qpid.proton4j.buffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.junit.Test;

public class ProtonByteBufferAllocatorTest {

    @Test
    public void testAllocate() {
        ProtonByteBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        assertNotNull(buffer);
        assertEquals(ProtonByteBuffer.DEFAULT_CAPACITY, buffer.capacity());
        assertEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.maxCapacity());
    }

    @Test
    public void testAllocateWithInitialCapacity() {
        ProtonByteBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(1024);

        assertNotNull(buffer);
        assertNotEquals(ProtonByteBuffer.DEFAULT_CAPACITY, buffer.capacity());
        assertEquals(1024, buffer.capacity());
        assertEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.maxCapacity());
    }

    @Test
    public void testAllocateWithInvalidInitialCapacity() {
        try {
            ProtonByteBufferAllocator.DEFAULT.allocate(-1);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAllocateWithInitialAndMaximumCapacity() {
        ProtonByteBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(1024, 2048);

        assertNotNull(buffer);
        assertNotEquals(ProtonByteBuffer.DEFAULT_CAPACITY, buffer.capacity());
        assertNotEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.maxCapacity());

        assertEquals(1024, buffer.capacity());
        assertEquals(2048, buffer.maxCapacity());
    }

    @Test
    public void testAllocateWithInvalidInitialAndMaximimCapacity() {
        try {
            ProtonByteBufferAllocator.DEFAULT.allocate(64, 32);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}

        try {
            ProtonByteBufferAllocator.DEFAULT.allocate(-1, 64);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}

        try {
            ProtonByteBufferAllocator.DEFAULT.allocate(-1, -1);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}

        try {
            ProtonByteBufferAllocator.DEFAULT.allocate(64, -1);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testWrapByteArray() {
        byte[] source = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ProtonByteBuffer buffer = ProtonByteBufferAllocator.DEFAULT.wrap(source);

        assertNotNull(buffer);
        assertNotEquals(ProtonByteBuffer.DEFAULT_CAPACITY, buffer.capacity());
        assertNotEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.maxCapacity());

        assertEquals(source.length, buffer.capacity());
        assertEquals(source.length, buffer.maxCapacity());

        assertSame(source, buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }
}
