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

    Boolean readBoolean(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    boolean readBoolean(ProtonBuffer buffer, DecoderState state, boolean defaultValue) throws DecodeException;

    Byte readByte(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    byte readByte(ProtonBuffer buffer, DecoderState state, byte defaultValue) throws DecodeException;

    UnsignedByte readUnsignedByte(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    byte readUnsignedByte(ProtonBuffer buffer, DecoderState state, byte defaultValue) throws DecodeException;

    Character readCharacter(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    char readCharacter(ProtonBuffer buffer, DecoderState state, char defaultValue) throws DecodeException;

    Decimal32 readDecimal32(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    Decimal64 readDecimal64(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    Decimal128 readDecimal128(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    Short readShort(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    short readShort(ProtonBuffer buffer, DecoderState state, short defaultValue) throws DecodeException;

    UnsignedShort readUnsignedShort(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    short readUnsignedShort(ProtonBuffer buffer, DecoderState state, short defaultValue) throws DecodeException;

    int readUnsignedShort(ProtonBuffer buffer, DecoderState state, int defaultValue) throws DecodeException;

    Integer readInteger(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    int readInteger(ProtonBuffer buffer, DecoderState state, int defaultValue) throws DecodeException;

    UnsignedInteger readUnsignedInteger(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    int readUnsignedInteger(ProtonBuffer buffer, DecoderState state, int defaultValue) throws DecodeException;

    long readUnsignedInteger(ProtonBuffer buffer, DecoderState state, long defaultValue) throws DecodeException;

    Long readLong(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    long readLong(ProtonBuffer buffer, DecoderState state, long defaultValue) throws DecodeException;

    UnsignedLong readUnsignedLong(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    long readUnsignedLong(ProtonBuffer buffer, DecoderState state, long defaultValue) throws DecodeException;

    Float readFloat(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    float readFloat(ProtonBuffer buffer, DecoderState state, float defaultValue) throws DecodeException;

    Double readDouble(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    double readDouble(ProtonBuffer buffer, DecoderState state, double defaultValue) throws DecodeException;

    Binary readBinary(ProtonBuffer buffer, DecoderState state) throws DecodeException;

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

    String readString(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    Symbol readSymbol(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    String readSymbol(ProtonBuffer buffer, DecoderState state, String defaultValue) throws DecodeException;

    Long readTimestamp(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    long readTimestamp(ProtonBuffer buffer, DecoderState state, long defaultValue) throws DecodeException;

    UUID readUUID(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    Object readObject(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    <T> T readObject(ProtonBuffer buffer, DecoderState state, final Class<T> clazz) throws DecodeException;

    <T> T[] readMultiple(ProtonBuffer buffer, DecoderState state, final Class<T> clazz) throws DecodeException;

    <K,V> Map<K, V> readMap(ProtonBuffer buffer, DecoderState state) throws DecodeException;

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
     * @throws DecodeException
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
     * @throws DecodeException
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
