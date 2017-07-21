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
import org.apache.qpid.proton4j.codec.PrimitiveArrayTypeEncoder;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Array types to a byte stream.
 */
public class ArrayTypeEncoder implements PrimitiveArrayTypeEncoder {

    @Override
    public void writeType(ByteBuf buffer, EncoderState state, Object value) {
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

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Object[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, boolean[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, byte[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, short[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, int[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, long[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, float[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, double[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, char[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Decimal32[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Decimal64[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Decimal128[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Symbol[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UUID[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UnsignedByte[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UnsignedShort[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UnsignedInteger[] value) {
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UnsignedLong[] value) {
    }

}
