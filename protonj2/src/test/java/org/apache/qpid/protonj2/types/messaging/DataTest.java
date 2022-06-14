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
package org.apache.qpid.protonj2.types.messaging;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.messaging.Section.SectionType;
import org.junit.jupiter.api.Test;

public class DataTest {

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new Data((Binary) null).toString());
    }

    @Test
    public void testGetDataFromEmptySection() {
        assertNull(new Data((byte[]) null).getValue());
    }

    @Test
    public void testCopyFromEmptyProtonBuffer() {
        assertNull(new Data((ProtonBuffer) null).copy().getBinary());
    }

    @Test
    public void testCopyFromEmpty() {
        assertNull(new Data((Binary) null).copy().getBinary());
    }

    @Test
    public void testCopy() {
        byte[] bytes = new byte[] { 1 };
        Binary binary = new Binary(bytes);
        Data data = new Data(binary);
        Data copy = data.copy();

        assertNotNull(copy.getValue());
        assertNotSame(data.getValue(), copy.getValue());
    }

    @Test
    public void testHashCode() {
        byte[] bytes = new byte[] { 1 };
        Binary binary = new Binary(bytes);
        Data data = new Data(binary);
        Data copy = data.copy();

        assertNotNull(copy.getValue());
        assertNotSame(data.getValue(), copy.getValue());

        assertEquals(data.hashCode(), copy.hashCode());

        Data second = new Data(new byte[] { 1, 2, 3 });

        assertNotEquals(data.hashCode(), second.hashCode());

        assertNotEquals(new Data((Binary) null).hashCode(), data.hashCode());
        assertEquals(new Data((Binary) null).hashCode(), new Data((ProtonBuffer) null).hashCode());
    }

    @Test
    public void testHashCodeWithProtonBuffer() {
        byte[] bytes = new byte[] { 1 };
        Data data = new Data(bytes);
        Data copy = data.copy();

        assertNotNull(copy.getValue());
        assertNotSame(data.getValue(), copy.getValue());

        assertEquals(data.hashCode(), copy.hashCode());

        Data second = new Data(new byte[] { 1, 2, 3 });

        assertNotEquals(data.hashCode(), second.hashCode());

        assertNotEquals(new Data((ProtonBuffer) null).hashCode(), data.hashCode());
        assertEquals(new Data((ProtonBuffer) null).hashCode(), new Data((ProtonBuffer) null).hashCode());
    }

    @Test
    public void testEquals() {
        byte[] bytes = new byte[] { 1 };
        Binary binary = new Binary(bytes);
        Data data = new Data(binary);
        Data copy = data.copy();

        assertNotNull(copy.getValue());
        assertNotSame(data.getValue(), copy.getValue());

        assertEquals(data, data);
        assertEquals(data, copy);

        Data second = new Data(ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 1, 2, 3 }));
        Data third = new Data(new byte[] { 1, 2, 3 }, 0, 3);
        Data fourth = new Data(new byte[] { 1, 2, 3 }, 0, 1);
        Data fifth = new Data(null, 0, 0);

        assertNotEquals(data, second);
        assertNotEquals(data, third);
        assertNotEquals(data, fifth);
        assertEquals(data, fourth);
        assertFalse(data.equals(null));
        assertNotEquals(data, "not a data");
        assertNotEquals(data, new Data((Binary) null));
        assertNotEquals(new Data((Binary) null), data);
        assertEquals(new Data((Binary) null), new Data((ProtonBuffer) null));
    }

    @Test
    public void testEqualsWithoutBinary() {
        byte[] bytes = new byte[] { 1 };
        Data data = new Data(bytes);
        Data copy = data.copy();

        assertNotNull(copy.getValue());
        assertNotSame(data.getValue(), copy.getValue());

        assertEquals(data, data);
        assertEquals(data, copy);

        Data second = new Data(ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 1, 2, 3 }));
        Data third = new Data(new byte[] { 1, 2, 3 }, 0, 3);
        Data fourth = new Data(new byte[] { 1, 2, 3 }, 0, 1);
        Data fifth = new Data(null, 0, 0);

        assertNotEquals(data, second);
        assertNotEquals(data, third);
        assertNotEquals(data, fifth);
        assertEquals(data, fourth);
        assertFalse(data.equals(null));
        assertNotEquals(data, "not a data");
        assertNotEquals(data, new Data((byte[]) null));
        assertNotEquals(new Data((ProtonBuffer) null), data);
        assertEquals(new Data((byte[]) null), new Data((ProtonBuffer) null));
    }

    @Test
    public void testGetValueWhenUsingAnArrayView() {
        Data view = new Data(new byte[] { 1, 2, 3 }, 0, 1);

        assertArrayEquals(new byte[] {1}, view.getValue());
    }

    @Test
    public void testGetType() {
        assertEquals(SectionType.Data, new Data((byte[]) null).getType());
    }
}
