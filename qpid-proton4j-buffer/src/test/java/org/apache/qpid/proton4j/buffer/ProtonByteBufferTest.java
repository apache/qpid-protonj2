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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Test behavior of the built in ProtonByteBuffer implementation.
 */
public class ProtonByteBufferTest {

    @Test
    public void testDefaultConstructor() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getReadableBytes());
        assertEquals(ProtonByteBuffer.DEFAULT_CAPACITY, buffer.capacity());
        assertEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.maxCapacity());

        assertTrue(buffer.hasArray());
        assertNotNull(buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }

    @Test
    public void testConstructorCapacity() {
        ProtonBuffer buffer = new ProtonByteBuffer(ProtonByteBuffer.DEFAULT_CAPACITY + 10);

        assertEquals(0, buffer.getReadableBytes());
        assertEquals(ProtonByteBuffer.DEFAULT_CAPACITY + 10, buffer.capacity());
        assertEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.maxCapacity());

        assertTrue(buffer.hasArray());
        assertNotNull(buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorCapacityExceptions() {
        new ProtonByteBuffer(-1);
    }

    @Test
    public void testConstructorCapacityMaxCapacity() {
        ProtonBuffer buffer = new ProtonByteBuffer(
            ProtonByteBuffer.DEFAULT_CAPACITY + 10, ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY - 100);

        assertEquals(0, buffer.getReadableBytes());
        assertEquals(ProtonByteBuffer.DEFAULT_CAPACITY + 10, buffer.capacity());
        assertEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY - 100, buffer.maxCapacity());

        assertTrue(buffer.hasArray());
        assertNotNull(buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }

    @Test
    public void testConstructorCapacityMaxCapacityExceptions() {

        try {
            new ProtonByteBuffer(-1, ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}

        try {
            new ProtonByteBuffer(ProtonByteBuffer.DEFAULT_CAPACITY, -1);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}

        try {
            new ProtonByteBuffer(100, 10);
            fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testConstructorByteArray() {
        byte[] source = new byte[ProtonByteBuffer.DEFAULT_CAPACITY + 10];

        ProtonBuffer buffer = new ProtonByteBuffer(source);

        assertEquals(0, buffer.getReadableBytes());
        assertEquals(ProtonByteBuffer.DEFAULT_CAPACITY + 10, buffer.capacity());
        assertEquals(ProtonByteBuffer.DEFAULT_MAXIMUM_CAPACITY, buffer.maxCapacity());

        assertTrue(buffer.hasArray());
        assertSame(source, buffer.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }

    @Test
    public void testIncreaseCapacity() {
        byte[] source = new byte[100];

        ProtonBuffer buffer = new ProtonByteBuffer(source);
        assertEquals(100, buffer.capacity());
        assertTrue(buffer.hasArray());
        assertSame(source, buffer.getArray());

        buffer.capacity(200);
        assertEquals(200, buffer.capacity());
        assertTrue(buffer.hasArray());
        assertNotSame(source, buffer.getArray());

        source = buffer.getArray();

        buffer.capacity(200);
        assertEquals(200, buffer.capacity());
        assertTrue(buffer.hasArray());
        assertSame(source, buffer.getArray());
    }

    @Test
    public void testDecreaseCapacity() {
        byte[] source = new byte[100];

        ProtonBuffer buffer = new ProtonByteBuffer(source);
        assertEquals(100, buffer.capacity());
        assertTrue(buffer.hasArray());
        assertSame(source, buffer.getArray());

        buffer.capacity(50);
        assertEquals(50, buffer.capacity());
        assertTrue(buffer.hasArray());
        assertNotSame(source, buffer.getArray());
    }

    @Test
    public void testCopyBuffer() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getReadableBytes());
        assertTrue(buffer.hasArray());

        ProtonBuffer duplicate = buffer.duplicate();

        assertSame(buffer.getArray(), duplicate.getArray());
        assertEquals(0, buffer.getArrayOffset());
    }

    //----- Write Method Tests -----------------------------------------------//

    @Test
    public void testWriteByte() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeByte((byte) 56);

        assertEquals(1, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(56, buffer.readByte());

        assertEquals(1, buffer.getWriteIndex());
        assertEquals(1, buffer.getReadIndex());

        assertEquals(0, buffer.getReadableBytes());
    }

    @Test
    public void testWriteBoolean() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeBoolean(true);

        assertEquals(1, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(true, buffer.readBoolean());

        assertEquals(1, buffer.getWriteIndex());
        assertEquals(1, buffer.getReadIndex());

        assertEquals(0, buffer.getReadableBytes());
    }

    @Test
    public void testWriteShort() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeShort((short) 42);

        assertEquals(2, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(42, buffer.readShort());

        assertEquals(2, buffer.getWriteIndex());
        assertEquals(2, buffer.getReadIndex());

        assertEquals(0, buffer.getReadableBytes());
    }

    @Test
    public void testWriteInt() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeInt(72);

        assertEquals(4, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(72, buffer.readInt());

        assertEquals(4, buffer.getWriteIndex());
        assertEquals(4, buffer.getReadIndex());

        assertEquals(0, buffer.getReadableBytes());
    }

    @Test
    public void testWriteLong() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeLong(500l);

        assertEquals(8, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(500l, buffer.readLong());

        assertEquals(8, buffer.getWriteIndex());
        assertEquals(8, buffer.getReadIndex());

        assertEquals(0, buffer.getReadableBytes());
    }

    @Test
    public void testWriteFloat() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeFloat(35.5f);

        assertEquals(4, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(35.5f, buffer.readFloat(), 0.4f);

        assertEquals(4, buffer.getWriteIndex());
        assertEquals(4, buffer.getReadIndex());

        assertEquals(0, buffer.getReadableBytes());
    }

    @Test
    public void testWriteDouble() {
        ProtonBuffer buffer = new ProtonByteBuffer();

        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        buffer.writeDouble(1.66);

        assertEquals(8, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());

        assertEquals(1.66, buffer.readDouble(), 0.1);

        assertEquals(8, buffer.getWriteIndex());
        assertEquals(8, buffer.getReadIndex());

        assertEquals(0, buffer.getReadableBytes());
    }

    //----- Capacity Expansion Tests -----------------------------------------//

    @Test
    public void testCapacityIncreasesWhenWritesExceedCurrent() {
        ProtonBuffer buffer = new ProtonByteBuffer(10);

        assertTrue(buffer.hasArray());

        assertEquals(10, buffer.capacity());
        assertEquals(10, buffer.getArray().length);
        assertEquals(Integer.MAX_VALUE, buffer.maxCapacity());

        for (int i = 1; i <= 9; ++i) {
            buffer.writeByte(i);
        }

        assertEquals(10, buffer.capacity());

        buffer.writeByte(10);

        assertEquals(10, buffer.capacity());
        assertEquals(10, buffer.getArray().length);

        buffer.writeByte(11);

        assertTrue(buffer.capacity() > 10);
        assertTrue(buffer.getArray().length > 10);

        assertEquals(11, buffer.getReadableBytes());

        for (int i = 1; i < 12; ++i) {
            assertEquals(i, buffer.readByte());
        }
    }
}
