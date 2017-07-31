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
package org.apache.qpid.proton4j.codec.encoders.primitives;

import java.util.UUID;

import org.apache.qpid.proton4j.amqp.Decimal128;
import org.apache.qpid.proton4j.amqp.Decimal32;
import org.apache.qpid.proton4j.amqp.Decimal64;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedByte;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveArrayTypeEncoder;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Array types to a byte stream.
 */
public class ArrayTypeEncoder implements PrimitiveArrayTypeEncoder {

    @Override
    public void writeType(ByteBuf buffer, EncoderState state, Object value) {
        if (!value.getClass().isArray()) {
            throw new IllegalArgumentException("Expected Array type but got: " + value.getClass().getSimpleName());
        }

        buffer.writeByte(EncodingCodes.ARRAY32);

        // TODO - Need to handle arrays of arrays

        Class<?> componentType = value.getClass().getComponentType();
        if (componentType.isPrimitive()) {
            if (componentType == Boolean.TYPE) {
                writeArray(buffer, state, (boolean[]) value);
            } else if (componentType == Byte.TYPE) {
                writeArray(buffer, state, (byte[]) value);
            } else if (componentType == Short.TYPE) {
                writeArray(buffer, state, (short[]) value);
            } else if (componentType == Integer.TYPE) {
                writeArray(buffer, state, (int[]) value);
            } else if (componentType == Long.TYPE) {
                writeArray(buffer, state, (long[]) value);
            } else if (componentType == Float.TYPE) {
                writeArray(buffer, state, (float[]) value);
            } else if (componentType == Double.TYPE) {
                writeArray(buffer, state, (double[]) value);
            } else if (componentType == Character.TYPE) {
                writeArray(buffer, state, (char[]) value);
            } else {
                throw new IllegalArgumentException(
                    "Cannot write arrays of type " + componentType.getName());
            }
        } else {
            writeArray(buffer, state, (Object[]) value);
        }
    }

    @Override
    public void writeValue(ByteBuf buffer, EncoderState state, Object value) {
    }

    //---- One Dimensional Optimized Array of Primitive Write methods --------//

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Object[] value) {
        // TODO - Handle primitive arrays or fallback to normal write method
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, boolean[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        int size = (Byte.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        buffer.writeInt(size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.BOOLEAN);
        for (boolean bool : value) {
            buffer.writeByte(bool ? 1 : 0);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, byte[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        int size = (Byte.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        buffer.writeInt(size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.BYTE);
        for (byte byteVal : value) {
            buffer.writeByte(byteVal);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, short[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        int size = (Short.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        buffer.writeInt(size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.SHORT);
        for (short shortValue : value) {
            buffer.writeShort(shortValue);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, int[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Integer.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given int array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.INT);
        for (int intValue : value) {
            buffer.writeInt(intValue);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, long[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Long.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given long array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.LONG);
        for (long longValue : value) {
            buffer.writeLong(longValue);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, float[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Long.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given float array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.FLOAT);
        for (float floatValue : value) {
            buffer.writeFloat(floatValue);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, double[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Long.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given double array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.DOUBLE);
        for (double doubleValue : value) {
            buffer.writeDouble(doubleValue);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, char[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Integer.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given char array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.CHAR);
        for (char charValue : value) {
            buffer.writeInt(charValue & 0xffff);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Decimal32[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Integer.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given Decimal32 array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.DECIMAL32);
        for (Decimal32 dec32Value : value) {
            buffer.writeInt(dec32Value.getBits());
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Decimal64[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Long.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given Decimal64 array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.DECIMAL64);
        for (Decimal64 dec64Value : value) {
            buffer.writeLong(dec64Value.getBits());
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Decimal128[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Long.BYTES * value.length * 2) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given Decimal128 array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.DECIMAL128);
        for (Decimal128 dec128Value : value) {
            buffer.writeLong(dec128Value.getMostSignificantBits());
            buffer.writeLong(dec128Value.getLeastSignificantBits());
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Symbol[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        //
        // Symbol types are variable sized values so we write the payload
        // and then we write the size using the result.
        int startIndex = buffer.writerIndex();

        // Reserve space for the size
        buffer.writeInt(0);

        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.SYM32);
        for (Symbol symbol : value) {
            byte[] symbolByte = symbol.getBytes();
            buffer.writeInt(symbolByte.length);
            buffer.writeBytes(symbolByte);
        }

        // Move back and write the size
        int endIndex = buffer.writerIndex();

        long size = endIndex - startIndex;
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given Symbol array, encoded size to large: " + size);
        }

        buffer.setInt(startIndex, (int) size);
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UUID[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Long.BYTES * value.length * 2) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given UUID array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.UUID);
        for (UUID uuid : value) {
            buffer.writeLong(uuid.getMostSignificantBits());
            buffer.writeLong(uuid.getLeastSignificantBits());
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UnsignedByte[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Byte.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        buffer.writeInt((int) size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.UBYTE);
        for (UnsignedByte byteVal : value) {
            buffer.writeByte(byteVal.byteValue());
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UnsignedShort[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        int size = (Short.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        buffer.writeInt(size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.USHORT);
        for (UnsignedShort shortVal : value) {
            buffer.writeShort(shortVal.shortValue());
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UnsignedInteger[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Integer.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given UnsignedInteger array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.UINT);
        for (UnsignedInteger intValue : value) {
            buffer.writeInt(intValue.intValue());
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UnsignedLong[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Long.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given UnsignedLong array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.ULONG);
        for (UnsignedLong longValue : value) {
            buffer.writeLong(longValue.longValue());
        }
    }
}
