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
package org.apache.qpid.protonj2.codec;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Decimal128;
import org.apache.qpid.protonj2.types.Decimal32;
import org.apache.qpid.protonj2.types.Decimal64;
import org.apache.qpid.protonj2.types.DeliveryTag;
import org.apache.qpid.protonj2.types.DescribedType;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.UnsignedShort;

/**
 * Encode AMQP types into binary streams
 */
public interface Encoder {

    /**
     * Creates a new {@link EncoderState} instance that can be used when interacting with the
     * Encoder.  For encoding that occurs on more than one thread while sharing a single
     * {@link Encoder} instance a different state object per thread is required as the
     * {@link EncoderState} object can retain some state information during the encode process
     * that could be corrupted if more than one thread were to share a single instance.
     *
     * For single threaded encoding work the {@link Encoder} offers a utility
     * cached {@link EncoderState} API that will return the same instance on each call which can
     * reduce allocation overhead and make using the {@link Encoder} simpler.
     *
     * @return a newly constructed {@link EncoderState} instance.
     */
    EncoderState newEncoderState();

    /**
     * Return a singleton {@link EncoderState} instance that is meant to be shared within single threaded
     * encoder interactions.  If more than one thread makes use of this cache {@link EncoderState} the
     * results of any encoding done using this state object is not guaranteed to be correct.  The returned
     * instance will have its reset method called to ensure that any previously stored state data is cleared
     * before the next use.
     *
     * @return a cached {@link EncoderState} linked to this Encoder instance.
     */
    EncoderState getCachedEncoderState();

    /**
     * Write a Null type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeNull(ProtonBuffer buffer, EncoderState state) throws EncodeException;

    /**
     * Write a {@link Boolean} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeBoolean(ProtonBuffer buffer, EncoderState state, boolean value) throws EncodeException;

    /**
     * Write a {@link Boolean} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeBoolean(ProtonBuffer buffer, EncoderState state, Boolean value) throws EncodeException;

    /**
     * Write an {@link UnsignedByte} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeUnsignedByte(ProtonBuffer buffer, EncoderState state, UnsignedByte value) throws EncodeException;

    /**
     * Write an {@link UnsignedByte} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeUnsignedByte(ProtonBuffer buffer, EncoderState state, byte value) throws EncodeException;

    /**
     * Write a {@link UnsignedShort} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeUnsignedShort(ProtonBuffer buffer, EncoderState state, UnsignedShort value) throws EncodeException;

    /**
     * Write a {@link UnsignedShort} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeUnsignedShort(ProtonBuffer buffer, EncoderState state, short value) throws EncodeException;

    /**
     * Write a {@link UnsignedShort} type encoding to the given buffer using the provided value with
     * appropriate range checks to ensure invalid input is not accepted.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeUnsignedShort(ProtonBuffer buffer, EncoderState state, int value) throws EncodeException;

    /**
     * Write a {@link UnsignedInteger} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeUnsignedInteger(ProtonBuffer buffer, EncoderState state, UnsignedInteger value) throws EncodeException;

    /**
     * Write a {@link UnsignedInteger} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeUnsignedInteger(ProtonBuffer buffer, EncoderState state, byte value) throws EncodeException;

    /**
     * Write a {@link UnsignedInteger} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeUnsignedInteger(ProtonBuffer buffer, EncoderState state, int value) throws EncodeException;

    /**
     * Write a {@link UnsignedInteger} type encoding to the given buffer using the provided value with
     * appropriate range checks to ensure invalid input is not accepted.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeUnsignedInteger(ProtonBuffer buffer, EncoderState state, long value) throws EncodeException;

    /**
     * Write a {@link UnsignedLong} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeUnsignedLong(ProtonBuffer buffer, EncoderState state, UnsignedLong value) throws EncodeException;

    /**
     * Write a {@link UnsignedLong} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeUnsignedLong(ProtonBuffer buffer, EncoderState state, byte value) throws EncodeException;

    /**
     * Write a {@link UnsignedLong} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeUnsignedLong(ProtonBuffer buffer, EncoderState state, long value) throws EncodeException;

    /**
     * Write a {@link Byte} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeByte(ProtonBuffer buffer, EncoderState state, byte value) throws EncodeException;

    /**
     * Write a {@link Byte} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeByte(ProtonBuffer buffer, EncoderState state, Byte value) throws EncodeException;

    /**
     * Write a {@link Short} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeShort(ProtonBuffer buffer, EncoderState state, short value) throws EncodeException;

    /**
     * Write a {@link Short} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeShort(ProtonBuffer buffer, EncoderState state, Short value) throws EncodeException;

    /**
     * Write a {@link Integer} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeInteger(ProtonBuffer buffer, EncoderState state, int value) throws EncodeException;

    /**
     * Write a {@link Integer} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeInteger(ProtonBuffer buffer, EncoderState state, Integer value) throws EncodeException;

    /**
     * Write a {@link Long} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeLong(ProtonBuffer buffer, EncoderState state, long value) throws EncodeException;

    /**
     * Write a {@link Long} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeLong(ProtonBuffer buffer, EncoderState state, Long value) throws EncodeException;

    /**
     * Write a {@link Float} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeFloat(ProtonBuffer buffer, EncoderState state, float value) throws EncodeException;

    /**
     * Write a {@link Float} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeFloat(ProtonBuffer buffer, EncoderState state, Float value) throws EncodeException;

    /**
     * Write a {@link Double} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeDouble(ProtonBuffer buffer, EncoderState state, double value) throws EncodeException;

    /**
     * Write a {@link Double} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeDouble(ProtonBuffer buffer, EncoderState state, Double value) throws EncodeException;

    /**
     * Write a {@link Decimal32} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeDecimal32(ProtonBuffer buffer, EncoderState state, Decimal32 value) throws EncodeException;

    /**
     * Write a {@link Decimal64} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeDecimal64(ProtonBuffer buffer, EncoderState state, Decimal64 value) throws EncodeException;

    /**
     * Write a {@link Decimal128} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeDecimal128(ProtonBuffer buffer, EncoderState state, Decimal128 value) throws EncodeException;

    /**
     * Write a {@link Character} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeCharacter(ProtonBuffer buffer, EncoderState state, char value) throws EncodeException;

    /**
     * Write a {@link Character} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeCharacter(ProtonBuffer buffer, EncoderState state, Character value) throws EncodeException;

    /**
     * Write a Time stamp type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeTimestamp(ProtonBuffer buffer, EncoderState state, long value) throws EncodeException;

    /**
     * Write a Time stamp type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeTimestamp(ProtonBuffer buffer, EncoderState state, Date value) throws EncodeException;

    /**
     * Write a {@link UUID} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeUUID(ProtonBuffer buffer, EncoderState state, UUID value) throws EncodeException;

    /**
     * Writes the contents of the given {@link Binary} value into the provided {@link ProtonBuffer}
     * instance as an AMQP Binary type.
     * <p>
     * If the provided value to write is null an AMQP null type is encoded into the target buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeBinary(ProtonBuffer buffer, EncoderState state, Binary value) throws EncodeException;

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
     *
     * @throws EncodeException if an error occurs while performing the encode
     */
    void writeBinary(ProtonBuffer buffer, EncoderState state, ProtonBuffer value) throws EncodeException;

    /**
     * Writes the contents of the given {@link byte[]} value into the provided {@link ProtonBuffer}
     * instance as an AMQP Binary type.
     * <p>
     * If the provided value to write is null an AMQP null type is encoded into the target buffer.
     *
     * @param buffer
     *      the target buffer where the binary value is to be encoded
     * @param state
     *      the {@link EncoderState} instance that manages the calling threads state tracking.
     * @param value
     *      the {@link ProtonBuffer} value to be encoded as an AMQP binary instance.
     *
     * @throws EncodeException if an error occurs while performing the encode
     */
    void writeBinary(ProtonBuffer buffer, EncoderState state, byte[] value) throws EncodeException;

    /**
     * Write a {@link String} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeString(ProtonBuffer buffer, EncoderState state, String value) throws EncodeException;

    /**
     * Write a {@link Symbol} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeSymbol(ProtonBuffer buffer, EncoderState state, Symbol value) throws EncodeException;

    /**
     * Write a {@link Symbol} type encoding to the given buffer.  The provided {@link String} instance should
     * contain only ASCII characters and the encoder should throw an {@link EncodeException} if a non-ASCII
     * character is encountered.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeSymbol(ProtonBuffer buffer, EncoderState state, String value) throws EncodeException;

    /**
     * Write a {@link List} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    <T> void writeList(ProtonBuffer buffer, EncoderState state, List<T> value) throws EncodeException;

    /**
     * Write a {@link Map} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    <K, V> void writeMap(ProtonBuffer buffer, EncoderState state, Map<K, V> value) throws EncodeException;

    /**
     * Writes the contents of the given {@link DeliveryTag} value into the provided {@link ProtonBuffer}
     * instance as an AMQP Binary type.
     * <p>
     * If the provided value to write is null an AMQP null type is encoded into the target buffer.
     *
     * @param buffer
     *      the target buffer where the binary value is to be encoded
     * @param state
     *      the {@link EncoderState} instance that manages the calling threads state tracking.
     * @param value
     *      the {@link DeliveryTag} value to be encoded as an AMQP binary instance.
     *
     * @throws EncodeException if an error occurs while performing the encode
     */
    void writeDeliveryTag(ProtonBuffer buffer, EncoderState state, DeliveryTag value) throws EncodeException;

    /**
     * Write a {@link DescribedType} type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeDescribedType(ProtonBuffer buffer, EncoderState state, DescribedType value) throws EncodeException;

    /**
     * Write the proper type encoding for the provided {@link Object} to the given buffer if an {@link TypeEncoder}
     * can be found for it in the collection of registered type encoders..
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeObject(ProtonBuffer buffer, EncoderState state, Object value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, boolean[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, byte[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, short[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, int[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, long[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, float[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, double[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, char[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, Object[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, Decimal32[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, Decimal64[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, Decimal128[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, Symbol[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, UnsignedByte[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, UnsignedShort[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, UnsignedInteger[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, UnsignedLong[] value) throws EncodeException;

    /**
     * Write the given array as with the proper array type encoding to the given buffer.
     *
     * @param buffer
     * 		The buffer where the write operation is targeted
     * @param state
     *      The {@link EncoderState} to use for any intermediate encoding work.
     * @param value
     * 		The value to be encoded into the provided buffer.
     *
     * @throws EncodeException if an error occurs during the encode operation.
     */
    void writeArray(ProtonBuffer buffer, EncoderState state, UUID[] value) throws EncodeException;

    /**
     * Register a {@link DescribedTypeEncoder} which can be used when writing custom types using this
     * encoder.  When an Object write is performed the type encoder registry will be consulted in order
     * to find the best match for the given {@link Object} instance.
     *
     * @param <V> The type that the encoder handles.
     *
     * @param encoder
     * 		A new {@link DescribedTypeEncoder} that will be used when encoding its matching type.
     *
     * @return this {@link Encoder} instance.
     *
     * @throws EncodeException if an error occurs while adding the encoder to the registry.
     */
    <V> Encoder registerDescribedTypeEncoder(DescribedTypeEncoder<V> encoder) throws EncodeException;

    /**
     * Lookup a {@link TypeEncoder} that would be used to encode the given {@link Object}.
     *
     * @param value
     * 		The value which should be used to resolve the {@link TypeEncoder} that encodes it.
     *
     * @return the matching {@link TypeEncoder} for the given value or null if no match found.
     */
    TypeEncoder<?> getTypeEncoder(Object value);

    /**
     * Lookup a {@link TypeEncoder} that would be used to encode the given {@link Class}.
     *
     * @param typeClass
     * 		The {@link Class} which should be used to resolve the {@link TypeEncoder} that encodes it.
     *
     * @return the matching {@link TypeEncoder} for the given value or null if no match found.
     */
    TypeEncoder<?> getTypeEncoder(Class<?> typeClass);

}
