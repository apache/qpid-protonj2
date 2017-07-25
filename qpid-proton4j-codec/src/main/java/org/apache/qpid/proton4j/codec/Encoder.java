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
package org.apache.qpid.proton4j.codec;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Decimal128;
import org.apache.qpid.proton4j.amqp.Decimal32;
import org.apache.qpid.proton4j.amqp.Decimal64;
import org.apache.qpid.proton4j.amqp.DescribedType;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedByte;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.UnsignedShort;

import io.netty.buffer.ByteBuf;

/**
 * Encode AMQP types into binary streams
 */
public interface Encoder {

    EncoderState newEncoderState();

    void writeNull(ByteBuf buffer, EncoderState state);

    void writeBoolean(ByteBuf buffer, EncoderState state, boolean value);

    void writeBoolean(ByteBuf buffer, EncoderState state, Boolean value);

    void writeUnsignedByte(ByteBuf buffer, EncoderState state, UnsignedByte value);

    void writeUnsignedShort(ByteBuf buffer, EncoderState state, UnsignedShort value);

    void writeUnsignedInteger(ByteBuf buffer, EncoderState state, UnsignedInteger value);

    void writeUnsignedLong(ByteBuf buffer, EncoderState state, UnsignedLong value);

    void writeByte(ByteBuf buffer, EncoderState state, byte value);

    void writeByte(ByteBuf buffer, EncoderState state, Byte value);

    void writeShort(ByteBuf buffer, EncoderState state, short value);

    void writeShort(ByteBuf buffer, EncoderState state, Short value);

    void writeInteger(ByteBuf buffer, EncoderState state, int value);

    void writeInteger(ByteBuf buffer, EncoderState state, Integer value);

    void writeLong(ByteBuf buffer, EncoderState state, long value);

    void writeLong(ByteBuf buffer, EncoderState state, Long value);

    void writeFloat(ByteBuf buffer, EncoderState state, float value);

    void writeFloat(ByteBuf buffer, EncoderState state, Float value);

    void writeDouble(ByteBuf buffer, EncoderState state, double value);

    void writeDouble(ByteBuf buffer, EncoderState state, Double value);

    void writeDecimal32(ByteBuf buffer, EncoderState state, Decimal32 value);

    void writeDecimal64(ByteBuf buffer, EncoderState state, Decimal64 value);

    void writeDecimal128(ByteBuf buffer, EncoderState state, Decimal128 value);

    void writeCharacter(ByteBuf buffer, EncoderState state, char value);

    void writeCharacter(ByteBuf buffer, EncoderState state, Character value);

    void writeTimestamp(ByteBuf buffer, EncoderState state, long value);
    void writeTimestamp(ByteBuf buffer, EncoderState state, Date value);

    void writeUUID(ByteBuf buffer, EncoderState state, UUID value);

    void writeBinary(ByteBuf buffer, EncoderState state, Binary value);

    void writeString(ByteBuf buffer, EncoderState state, String value);

    void writeSymbol(ByteBuf buffer, EncoderState state, Symbol value);

    <T> void writeList(ByteBuf buffer, EncoderState state, List<T> value);

    <K, V> void writeMap(ByteBuf buffer, EncoderState state, Map<K, V> value);

    void writeDescribedType(ByteBuf buffer, EncoderState state, DescribedType value);

    void writeArray(ByteBuf buffer, EncoderState state, boolean[] value);

    void writeArray(ByteBuf buffer, EncoderState state, byte[] value);

    void writeArray(ByteBuf buffer, EncoderState state, short[] value);

    void writeArray(ByteBuf buffer, EncoderState state, int[] value);

    void writeArray(ByteBuf buffer, EncoderState state, long[] value);

    void writeArray(ByteBuf buffer, EncoderState state, float[] value);

    void writeArray(ByteBuf buffer, EncoderState state, double[] value);

    void writeArray(ByteBuf buffer, EncoderState state, char[] value);

    void writeArray(ByteBuf buffer, EncoderState state, Object[] value);

    void writeArray(ByteBuf buffer, EncoderState state, Decimal32[] value);

    void writeArray(ByteBuf buffer, EncoderState state, Decimal64[] value);

    void writeArray(ByteBuf buffer, EncoderState state, Decimal128[] value);

    void writeArray(ByteBuf buffer, EncoderState state, Symbol[] value);

    void writeArray(ByteBuf buffer, EncoderState state, UnsignedByte[] value);

    void writeArray(ByteBuf buffer, EncoderState state, UnsignedShort[] value);

    void writeArray(ByteBuf buffer, EncoderState state, UnsignedInteger[] value);

    void writeArray(ByteBuf buffer, EncoderState state, UnsignedLong[] value);

    void writeArray(ByteBuf buffer, EncoderState state, UUID[] value);

    void writeObject(ByteBuf buffer, EncoderState state, Object value);

    <V> Encoder registerTypeEncoder(TypeEncoder<V> encoder);

    TypeEncoder<?> getTypeEncoder(Object value);

    TypeEncoder<?> getTypeEncoder(Class<?> typeClass);

}
