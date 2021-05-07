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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Decimal128;
import org.apache.qpid.protonj2.types.Decimal32;
import org.apache.qpid.protonj2.types.Decimal64;
import org.apache.qpid.protonj2.types.DeliveryTag;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.UnsignedShort;

/**
 * Decode AMQP types from a byte stream
 */
public interface Decoder {

    /**
     * Creates a new {@link DecoderState} instance that can be used when interacting with the
     * Decoder.  For decoding that occurs on more than one thread while sharing a single
     * {@link Decoder} instance a different state object per thread is required as the
     * {@link DecoderState} object can retain some state information during the decode process
     * that could be corrupted if more than one thread were to share a single instance.
     *
     * For single threaded decoding work the {@link Decoder} offers a utility
     * cached {@link DecoderState} API that will return the same instance on each call which can
     * reduce allocation overhead and make using the {@link Decoder} simpler.
     *
     * @return a newly constructed {@link EncoderState} instance.
     */
    DecoderState newDecoderState();

    /**
     * Return a singleton {@link DecoderState} instance that is meant to be shared within single threaded
     * decoder interactions.  If more than one thread makes use of this cached {@link DecoderState} the
     * results of any decoding done using this state object is not guaranteed to be correct.  The returned
     * instance will have its reset method called to ensure that any previously stored state data is cleared
     * before the next use.
     *
     * @return a cached {@link DecoderState} linked to this Decoder instance that has been reset.
     */
    DecoderState getCachedDecoderState();

    /**
     * Reads an encoded {@link Boolean} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    Boolean readBoolean(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link Byte} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    boolean readBoolean(ProtonBuffer buffer, DecoderState state, boolean defaultValue) throws DecodeException;

    /**
     * Reads an encoded {@link Byte} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    Byte readByte(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link Byte} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    byte readByte(ProtonBuffer buffer, DecoderState state, byte defaultValue) throws DecodeException;

    /**
     * Reads an encoded {@link UnsignedByte} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    UnsignedByte readUnsignedByte(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link UnsignedByte} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    byte readUnsignedByte(ProtonBuffer buffer, DecoderState state, byte defaultValue) throws DecodeException;

    /**
     * Reads an encoded {@link Character} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    Character readCharacter(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link Character} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    char readCharacter(ProtonBuffer buffer, DecoderState state, char defaultValue) throws DecodeException;

    /**
     * Reads an encoded {@link Decimal32} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    Decimal32 readDecimal32(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link Decimal64} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    Decimal64 readDecimal64(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link Decimal128} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    Decimal128 readDecimal128(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link Short} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    Short readShort(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link Short} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    short readShort(ProtonBuffer buffer, DecoderState state, short defaultValue) throws DecodeException;

    /**
     * Reads an encoded {@link UnsignedShort} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    UnsignedShort readUnsignedShort(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link UnsignedShort} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    short readUnsignedShort(ProtonBuffer buffer, DecoderState state, short defaultValue) throws DecodeException;

    /**
     * Reads an encoded {@link UnsignedShort} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    int readUnsignedShort(ProtonBuffer buffer, DecoderState state, int defaultValue) throws DecodeException;

    /**
     * Reads an encoded {@link Integer} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    Integer readInteger(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link Integer} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    int readInteger(ProtonBuffer buffer, DecoderState state, int defaultValue) throws DecodeException;

    /**
     * Reads an encoded {@link UnsignedInteger} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    UnsignedInteger readUnsignedInteger(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link UnsignedInteger} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    int readUnsignedInteger(ProtonBuffer buffer, DecoderState state, int defaultValue) throws DecodeException;

    /**
     * Reads an encoded {@link UnsignedInteger} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    long readUnsignedInteger(ProtonBuffer buffer, DecoderState state, long defaultValue) throws DecodeException;

    /**
     * Reads an encoded {@link Long} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    Long readLong(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link Long} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    long readLong(ProtonBuffer buffer, DecoderState state, long defaultValue) throws DecodeException;

    /**
     * Reads an encoded {@link UnsignedLong} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    UnsignedLong readUnsignedLong(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link UnsignedLong} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    long readUnsignedLong(ProtonBuffer buffer, DecoderState state, long defaultValue) throws DecodeException;

    /**
     * Reads an encoded {@link Float} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    Float readFloat(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link Float} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    float readFloat(ProtonBuffer buffer, DecoderState state, float defaultValue) throws DecodeException;

    /**
     * Reads an encoded {@link Double} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    Double readDouble(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link Double} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    double readDouble(ProtonBuffer buffer, DecoderState state, double defaultValue) throws DecodeException;

    /**
     * Reads an encoded {@link Binary} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    Binary readBinary(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link Binary} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source and returned in a {@link ProtonBuffer}.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    ProtonBuffer readBinaryAsBuffer(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * This method expects to read a {@link Binary} encoded type from the provided buffer and
     * constructs a {@link DeliveryTag} type that wraps the bytes encoded.  If the encoding is
     * a NULL AMQP type then this method returns <code>null</code>.
     *
     * @param buffer
     *      The buffer to read a Binary encoded value from
     * @param state
     *      The current encoding state.
     *
     * @return a new DeliveryTag instance or null if an AMQP NULL encoding is found.
     *
     * @throws DecodeException if an error occurs while decoding the {@link DeliveryTag} instance.
     */
    DeliveryTag readDeliveryTag(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link String} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    String readString(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link Symbol} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    Symbol readSymbol(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link String} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    String readSymbol(ProtonBuffer buffer, DecoderState state, String defaultValue) throws DecodeException;

    /**
     * Reads an encoded AMQP time stamp value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source and return a {@link Long} with the time value.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    Long readTimestamp(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded AMQP time stamp value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param defaultValue
     * 		A default value to return if the next encoded value is a Null encoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    long readTimestamp(ProtonBuffer buffer, DecoderState state, long defaultValue) throws DecodeException;

    /**
     * Reads an encoded {@link UUID} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    UUID readUUID(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded value from the given {@link ProtonBuffer} an return it as an {@link Object}
     * which the caller must then interpret.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not able to be decoded.
     */
    Object readObject(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded value from the given {@link ProtonBuffer} an return it as an {@link Object}
     * which the caller must then interpret.
     *
     * @param <T> the type that will be used when casting and returning the decoded value.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param clazz
     * 		The {@link Class} type that should be used to cast the returned value.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not able to be decoded.
     */
    <T> T readObject(ProtonBuffer buffer, DecoderState state, final Class<T> clazz) throws DecodeException;

    /**
     * Reads one or more encoded values from the given {@link ProtonBuffer} an return it as an array of
     * {@link Object} instances which the caller must then interpret.
     *
     * @param <T> the type that will be used when casting and returning the decoded value.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     * @param clazz
     * 		The {@link Class} type that should be used to cast the returned array.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not able to be decoded.
     */
    <T> T[] readMultiple(ProtonBuffer buffer, DecoderState state, final Class<T> clazz) throws DecodeException;

    /**
     * Reads an encoded {@link Map} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    <K,V> Map<K, V> readMap(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads an encoded {@link List} value from the given {@link ProtonBuffer} assuming that the
     * next value in the byte stream is that type.  The operation fails if the next encoded type is
     * not what was expected.  If the caller wishes to recover from failed decode attempt they should
     * mark the and reset the input to make a further read attempt.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the read operation takes place.
     * @param state
     * 		The {@link DecoderState} that the decoder can use when decoding.
     *
     * @return the value read from the provided byte source.
     *
     * @throws DecodeException if the value fails to decode is not of the expected type,
     */
    <V> List<V> readList(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads from the given {@link ProtonBuffer} instance and returns a {@link TypeDecoder} that can
     * read the next encoded AMQP type from the buffer's bytes.  If an error occurs attempting to read
     * and determine the next type decoder an {@link DecodeException} is thrown.
     *
     * @param buffer
     * 		The buffer to read from to determine the next {@link TypeDecoder} needed.
     * @param state
     *      The {@link DecoderState} value that can be used for intermediate decoding tasks.
     *
     * @return a {@link TypeDecoder} instance that can read the next type in the buffer.
     *
     * @throws DecodeException if an error occurs while reading the next type decoder.
     */
    TypeDecoder<?> readNextTypeDecoder(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Peeks ahead in the given {@link ProtonBuffer} instance and returns a {@link TypeDecoder} that can
     * read the next encoded AMQP type from the buffer's bytes.  If an error occurs attempting to read
     * and determine the next type decoder an {@link DecodeException} is thrown.  The underlying buffer
     * is not modified as a result of the peek operation and the returned {@link TypeDecoder} will fail
     * to properly read the type until the encoding bytes are read.
     *
     * @param buffer
     * 		The buffer to read from to determine the next {@link TypeDecoder} needed.
     * @param state
     *      The {@link DecoderState} value that can be used for intermediate decoding tasks.
     *
     * @return a {@link TypeDecoder} instance that can provide insight into the next type in the buffer.
     *
     * @throws DecodeException if an error occurs while peeking ahead for the next type decoder.
     */
    TypeDecoder<?> peekNextTypeDecoder(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Allows custom {@link DescribedTypeDecoder} instances to be registered with this {@link Decoder}
     * which will be used if the described type encoding is encountered during decode operations.
     *
     * @param <V> The type that the decoder reads.
     *
     * @param decoder
     * 		A {@link DescribedTypeDecoder} instance to be registered with this {@link Decoder}
     *
     * @return this {@link Decoder} instance.
     */
    <V> Decoder registerDescribedTypeDecoder(DescribedTypeDecoder<V> decoder);

}
