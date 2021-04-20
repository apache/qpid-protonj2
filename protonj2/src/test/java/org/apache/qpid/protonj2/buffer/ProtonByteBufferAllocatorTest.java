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
package org.apache.qpid.protonj2.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

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
    public void testOutputBufferWithInitialCapacity() {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.outputBuffer(1024);

        assertNotNull(buffer);
        assertNotEquals(ProtonByteBuffer.DEFAULT_CAPACITY, buffer.capacity());
        assertEquals(1024, buffer.capacity());
        assertEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.maxCapacity());
    }

    @Test
    public void testOutputBufferWithInitialAndMaximumCapacity() {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.outputBuffer(1024, 2048);

        assertNotNull(buffer);
        assertNotEquals(ProtonByteBuffer.DEFAULT_CAPACITY, buffer.capacity());
        assertNotEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.maxCapacity());

        assertEquals(1024, buffer.capacity());
        assertEquals(2048, buffer.maxCapacity());
    }

    @Test
    public void testWrapByteArray() {
        byte[] source = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.wrap(source);

        assertNotNull(buffer);
        assertNotEquals(ProtonByteBuffer.DEFAULT_CAPACITY, buffer.capacity());
        assertNotEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.maxCapacity());

        assertEquals(source.length, buffer.capacity());
        assertEquals(source.length, buffer.maxCapacity());

        assertSame(source, buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }

    @Test
    public void testWrapByteArrayWithOffsetAndLength() {
        byte[] source = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.wrap(source, 0, source.length);

        assertNotNull(buffer);
        assertNotEquals(ProtonByteBuffer.DEFAULT_CAPACITY, buffer.capacity());
        assertNotEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.capacity());

        assertEquals(source.length, buffer.capacity());
        assertEquals(source.length, buffer.maxCapacity());

        assertSame(source, buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }

    @Test
    public void testWrapByteArrayWithOffsetAndLengthSubset() {
        byte[] source = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.wrap(source, 1, source.length - 2);

        assertNotNull(buffer);
        assertNotEquals(ProtonByteBuffer.DEFAULT_CAPACITY, buffer.capacity());
        assertNotEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.capacity());

        assertEquals(source.length - 2, buffer.capacity());
        assertEquals(source.length - 2, buffer.maxCapacity());

        assertSame(source, buffer.getArray());
        assertEquals(1, buffer.getArrayOffset());
    }

    @Test
    public void testCannotWrapReadOnlyByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(1024).asReadOnlyBuffer();
        assertThrows(UnsupportedOperationException.class, () -> ProtonByteBufferAllocator.DEFAULT.wrap(buffer));
    }

    @Test
    public void testCannotWrapByteBufferWithoutArrayBacking() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

        assumeTrue(buffer.isDirect());
        assumeFalse(buffer.hasArray());

        assertThrows(UnsupportedOperationException.class, () -> ProtonByteBufferAllocator.DEFAULT.wrap(buffer));
    }
}
