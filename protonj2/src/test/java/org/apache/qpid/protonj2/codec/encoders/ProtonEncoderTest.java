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
package org.apache.qpid.protonj2.codec.encoders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedShort;
import org.junit.jupiter.api.Test;

class ProtonEncoderTest extends CodecTestSupport {

    @Test
    public void testCachedEncoderStateIsCached() throws IOException {
        EncoderState state1 = encoder.getCachedEncoderState();
        EncoderState state2 = encoder.getCachedEncoderState();

        assertTrue(state1 instanceof ProtonEncoderState);
        assertTrue(state1 instanceof ProtonEncoderState);

        assertSame(state1, state2);
    }

    @Test
    public void testWriteBooleanObject() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeBoolean(buffer, encoderState, Boolean.TRUE);
        encoder.writeBoolean(buffer, encoderState, (Boolean) null);
        encoder.writeBoolean(buffer, encoderState, Boolean.FALSE);

        assertEquals(3, buffer.getReadableBytes());
        assertEquals(buffer.getByte(0), EncodingCodes.BOOLEAN_TRUE);
        assertEquals(buffer.getByte(1), EncodingCodes.NULL);
        assertEquals(buffer.getByte(2), EncodingCodes.BOOLEAN_FALSE);
    }

    @Test
    public void testWriteBooleanPrimitive() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeBoolean(buffer, encoderState, true);
        encoder.writeBoolean(buffer, encoderState, false);

        assertEquals(2, buffer.getReadableBytes());
        assertEquals(buffer.getByte(0), EncodingCodes.BOOLEAN_TRUE);
        assertEquals(buffer.getByte(1), EncodingCodes.BOOLEAN_FALSE);
    }

    @Test
    public void testWriteUnsignedByteObject() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedByte(buffer, encoderState, UnsignedByte.valueOf((byte) 0));
        encoder.writeUnsignedByte(buffer, encoderState, (UnsignedByte) null);
        encoder.writeUnsignedByte(buffer, encoderState, UnsignedByte.valueOf((byte) 255));

        assertEquals(5, buffer.getReadableBytes());
        assertEquals(buffer.getByte(0), EncodingCodes.UBYTE);
        assertEquals(buffer.getByte(1), 0);
        assertEquals(buffer.getByte(2), EncodingCodes.NULL);
        assertEquals(buffer.getByte(3), EncodingCodes.UBYTE);
        assertEquals(buffer.getByte(4), (byte) 255);
    }

    @Test
    public void testWriteUnsignedBytePrimitive() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedByte(buffer, encoderState, (byte) 0);
        encoder.writeUnsignedByte(buffer, encoderState, (byte) 255);

        assertEquals(4, buffer.getReadableBytes());
        assertEquals(buffer.getByte(0), EncodingCodes.UBYTE);
        assertEquals(buffer.getByte(1), 0);
        assertEquals(buffer.getByte(2), EncodingCodes.UBYTE);
        assertEquals(buffer.getByte(3), (byte) 255);
    }

    @Test
    public void testWriteUnsignedShortObject() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedShort(buffer, encoderState, UnsignedShort.valueOf((short) 0));
        encoder.writeUnsignedShort(buffer, encoderState, (UnsignedShort) null);
        encoder.writeUnsignedShort(buffer, encoderState, UnsignedShort.valueOf((short) 65535));

        assertEquals(7, buffer.getReadableBytes());
        assertEquals(buffer.getByte(0), EncodingCodes.USHORT);
        assertEquals(buffer.getByte(1), 0);
        assertEquals(buffer.getByte(2), 0);
        assertEquals(buffer.getByte(3), EncodingCodes.NULL);
        assertEquals(buffer.getByte(4), EncodingCodes.USHORT);
        assertEquals(buffer.getByte(5), (byte) 255);
        assertEquals(buffer.getByte(6), (byte) 255);
    }

    @Test
    public void testWriteUnsignedShortPrimitive() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedShort(buffer, encoderState, (short) 0);
        encoder.writeUnsignedShort(buffer, encoderState, (short) 65535);

        assertEquals(6, buffer.getReadableBytes());
        assertEquals(buffer.getByte(0), EncodingCodes.USHORT);
        assertEquals(buffer.getByte(1), 0);
        assertEquals(buffer.getByte(2), 0);
        assertEquals(buffer.getByte(3), EncodingCodes.USHORT);
        assertEquals(buffer.getByte(4), (byte) 255);
        assertEquals(buffer.getByte(5), (byte) 255);
    }

    @Test
    public void testWriteUnsignedShortPrimitiveInt() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedShort(buffer, encoderState, 0);
        encoder.writeUnsignedShort(buffer, encoderState, -1);
        encoder.writeUnsignedShort(buffer, encoderState, 65535);

        assertEquals(7, buffer.getReadableBytes());
        assertEquals(buffer.getByte(0), EncodingCodes.USHORT);
        assertEquals(buffer.getByte(1), 0);
        assertEquals(buffer.getByte(2), 0);
        assertEquals(buffer.getByte(3), EncodingCodes.NULL);
        assertEquals(buffer.getByte(4), EncodingCodes.USHORT);
        assertEquals(buffer.getByte(5), (byte) 255);
        assertEquals(buffer.getByte(6), (byte) 255);
    }

    @Test
    public void testWriteUnsignedIntegerObject() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedInteger(buffer, encoderState, UnsignedInteger.valueOf(0));
        encoder.writeUnsignedInteger(buffer, encoderState, (UnsignedInteger) null);
        encoder.writeUnsignedInteger(buffer, encoderState, UnsignedInteger.valueOf(255));
        encoder.writeUnsignedInteger(buffer, encoderState, UnsignedInteger.valueOf(-1));

        assertEquals(9, buffer.getReadableBytes());
        assertEquals(buffer.getByte(0), EncodingCodes.UINT0);
        assertEquals(buffer.getByte(1), EncodingCodes.NULL);
        assertEquals(buffer.getByte(2), EncodingCodes.SMALLUINT);
        assertEquals(buffer.getByte(3), (byte) 255);
        assertEquals(buffer.getByte(4), EncodingCodes.UINT);
        assertEquals(buffer.getByte(5), (byte) 255);
        assertEquals(buffer.getByte(6), (byte) 255);
        assertEquals(buffer.getByte(7), (byte) 255);
        assertEquals(buffer.getByte(8), (byte) 255);
    }

    @Test
    public void testWriteUnsignedIntegerPrimitive() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedInteger(buffer, encoderState, 0);
        encoder.writeUnsignedInteger(buffer, encoderState, 255);
        encoder.writeUnsignedInteger(buffer, encoderState, -1);

        assertEquals(8, buffer.getReadableBytes());
        assertEquals(buffer.getByte(0), EncodingCodes.UINT0);
        assertEquals(buffer.getByte(1), EncodingCodes.SMALLUINT);
        assertEquals(buffer.getByte(2), (byte) 255);
        assertEquals(buffer.getByte(3), EncodingCodes.UINT);
        assertEquals(buffer.getByte(4), (byte) 255);
        assertEquals(buffer.getByte(5), (byte) 255);
        assertEquals(buffer.getByte(6), (byte) 255);
        assertEquals(buffer.getByte(7), (byte) 255);
    }

    @Test
    public void testWriteUnsignedIntegerPrimitiveLong() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        encoder.writeUnsignedInteger(buffer, encoderState, 0l);
        encoder.writeUnsignedInteger(buffer, encoderState, 255l);
        encoder.writeUnsignedInteger(buffer, encoderState, ((long) Integer.MAX_VALUE * 2) + 1);
        encoder.writeUnsignedInteger(buffer, encoderState, -1l);

        assertEquals(9, buffer.getReadableBytes());
        assertEquals(buffer.getByte(0), EncodingCodes.UINT0);
        assertEquals(buffer.getByte(1), EncodingCodes.SMALLUINT);
        assertEquals(buffer.getByte(2), (byte) 255);
        assertEquals(buffer.getByte(3), EncodingCodes.UINT);
        assertEquals(buffer.getByte(4), (byte) 255);
        assertEquals(buffer.getByte(5), (byte) 255);
        assertEquals(buffer.getByte(6), (byte) 255);
        assertEquals(buffer.getByte(7), (byte) 255);
        assertEquals(buffer.getByte(8), EncodingCodes.NULL);
    }
}
