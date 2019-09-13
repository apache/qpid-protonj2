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
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Encode AMQP types into binary streams
 */
public interface Encoder {

    EncoderState newEncoderState();

    void writeNull(ProtonBuffer buffer, EncoderState state);

    void writeBoolean(ProtonBuffer buffer, EncoderState state, boolean value);

    void writeBoolean(ProtonBuffer buffer, EncoderState state, Boolean value);

    void writeUnsignedByte(ProtonBuffer buffer, EncoderState state, UnsignedByte value);

    void writeUnsignedByte(ProtonBuffer buffer, EncoderState state, byte value);

    void writeUnsignedShort(ProtonBuffer buffer, EncoderState state, UnsignedShort value);

    void writeUnsignedShort(ProtonBuffer buffer, EncoderState state, short value);

    void writeUnsignedShort(ProtonBuffer buffer, EncoderState state, int value);

    void writeUnsignedInteger(ProtonBuffer buffer, EncoderState state, UnsignedInteger value);

    void writeUnsignedInteger(ProtonBuffer buffer, EncoderState state, byte value);

    void writeUnsignedInteger(ProtonBuffer buffer, EncoderState state, int value);

    void writeUnsignedInteger(ProtonBuffer buffer, EncoderState state, long value);

    void writeUnsignedLong(ProtonBuffer buffer, EncoderState state, UnsignedLong value);

    void writeUnsignedLong(ProtonBuffer buffer, EncoderState state, byte value);

    void writeUnsignedLong(ProtonBuffer buffer, EncoderState state, long value);

    void writeByte(ProtonBuffer buffer, EncoderState state, byte value);

    void writeByte(ProtonBuffer buffer, EncoderState state, Byte value);

    void writeShort(ProtonBuffer buffer, EncoderState state, short value);

    void writeShort(ProtonBuffer buffer, EncoderState state, Short value);

    void writeInteger(ProtonBuffer buffer, EncoderState state, int value);

    void writeInteger(ProtonBuffer buffer, EncoderState state, Integer value);

    void writeLong(ProtonBuffer buffer, EncoderState state, long value);

    void writeLong(ProtonBuffer buffer, EncoderState state, Long value);

    void writeFloat(ProtonBuffer buffer, EncoderState state, float value);

    void writeFloat(ProtonBuffer buffer, EncoderState state, Float value);

    void writeDouble(ProtonBuffer buffer, EncoderState state, double value);

    void writeDouble(ProtonBuffer buffer, EncoderState state, Double value);

    void writeDecimal32(ProtonBuffer buffer, EncoderState state, Decimal32 value);

    void writeDecimal64(ProtonBuffer buffer, EncoderState state, Decimal64 value);

    void writeDecimal128(ProtonBuffer buffer, EncoderState state, Decimal128 value);

    void writeCharacter(ProtonBuffer buffer, EncoderState state, char value);

    void writeCharacter(ProtonBuffer buffer, EncoderState state, Character value);

    void writeTimestamp(ProtonBuffer buffer, EncoderState state, long value);

    void writeTimestamp(ProtonBuffer buffer, EncoderState state, Date value);

    void writeUUID(ProtonBuffer buffer, EncoderState state, UUID value);

    void writeBinary(ProtonBuffer buffer, EncoderState state, Binary value);

    /**
     * Writes the contents of the given {@link ProtonBuffer} value into the provided {@link ProtonBuffer}
     * instance as an AMQP Binary type.  This method does not modify the read index of the value given such
     * that is can be read later or written again without needing to reset the read index manually.
     * <p>
     * If the provided value to write is null an AMQP null type is encoded into the target buffer.
     *
     * @param buffer
     *      the target buffer where the binary value is to be encoded
     * @param state
     *      the {@link EncoderState} instance that manages the calling threads state tracking.
     * @param value
     *      the {@link ProtonBuffer} value to be encoded as an AMQP binary instance.
     */
    void writeBinary(ProtonBuffer buffer, EncoderState state, ProtonBuffer value);

    void writeBinary(ProtonBuffer buffer, EncoderState state, byte[] value);

    void writeString(ProtonBuffer buffer, EncoderState state, String value);

    void writeSymbol(ProtonBuffer buffer, EncoderState state, Symbol value);

    void writeSymbol(ProtonBuffer buffer, EncoderState state, String value);

    <T> void writeList(ProtonBuffer buffer, EncoderState state, List<T> value);

    <K, V> void writeMap(ProtonBuffer buffer, EncoderState state, Map<K, V> value);

    void writeDescribedType(ProtonBuffer buffer, EncoderState state, DescribedType value);

    void writeObject(ProtonBuffer buffer, EncoderState state, Object value);

    void writeArray(ProtonBuffer buffer, EncoderState state, boolean[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, byte[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, short[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, int[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, long[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, float[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, double[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, char[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, Object[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, Decimal32[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, Decimal64[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, Decimal128[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, Symbol[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, UnsignedByte[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, UnsignedShort[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, UnsignedInteger[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, UnsignedLong[] value);

    void writeArray(ProtonBuffer buffer, EncoderState state, UUID[] value);

    <V> Encoder registerTypeEncoder(TypeEncoder<V> encoder);

    TypeEncoder<?> getTypeEncoder(Object value);

    TypeEncoder<?> getTypeEncoder(Class<?> typeClass);

}
