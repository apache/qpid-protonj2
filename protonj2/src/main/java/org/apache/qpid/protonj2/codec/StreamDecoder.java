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

import java.io.InputStream;
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
 * Decode AMQP types from a {@link InputStream}
 */
public interface StreamDecoder {

    /**
     * Creates a new {@link StreamDecoderState} instance that can be used when interacting with the
     * Decoder.  For decoding that occurs on more than one thread while sharing a single
     * {@link StreamDecoder} instance a different state object per thread is required as the
     * {@link StreamDecoderState} object can retain some state information during the decode process
     * that could be corrupted if more than one thread were to share a single instance.
     *
     * For single threaded decoding work the {@link StreamDecoder} offers a utility
     * cached {@link StreamDecoderState} API that will return the same instance on each call which can
     * reduce allocation overhead and make using the {@link StreamDecoder} simpler.
     *
     * @return a newly constructed {@link EncoderState} instance.
     */
    StreamDecoderState newDecoderState();

    /**
     * Return a singleton {@link StreamDecoderState} instance that is meant to be shared within single threaded
     * decoder interactions.  If more than one thread makes use of this cached {@link StreamDecoderState} the
     * results of any decoding done using this state object is not guaranteed to be correct.  The returned
     * instance will have its reset method called to ensure that any previously stored state data is cleared
     * before the next use.
     *
     * @return a cached {@link StreamDecoderState} linked to this Decoder instance that has been reset.
     */
    StreamDecoderState getCachedDecoderState();

    Boolean readBoolean(InputStream stream, StreamDecoderState state) throws DecodeException;

    boolean readBoolean(InputStream stream, StreamDecoderState state, boolean defaultValue) throws DecodeException;

    Byte readByte(InputStream stream, StreamDecoderState state) throws DecodeException;

    byte readByte(InputStream stream, StreamDecoderState state, byte defaultValue) throws DecodeException;

    UnsignedByte readUnsignedByte(InputStream stream, StreamDecoderState state) throws DecodeException;

    byte readUnsignedByte(InputStream stream, StreamDecoderState state, byte defaultValue) throws DecodeException;

    Character readCharacter(InputStream stream, StreamDecoderState state) throws DecodeException;

    char readCharacter(InputStream stream, StreamDecoderState state, char defaultValue) throws DecodeException;

    Decimal32 readDecimal32(InputStream stream, StreamDecoderState state) throws DecodeException;

    Decimal64 readDecimal64(InputStream stream, StreamDecoderState state) throws DecodeException;

    Decimal128 readDecimal128(InputStream stream, StreamDecoderState state) throws DecodeException;

    Short readShort(InputStream stream, StreamDecoderState state) throws DecodeException;

    short readShort(InputStream stream, StreamDecoderState state, short defaultValue) throws DecodeException;

    UnsignedShort readUnsignedShort(InputStream stream, StreamDecoderState state) throws DecodeException;

    short readUnsignedShort(InputStream stream, StreamDecoderState state, short defaultValue) throws DecodeException;

    int readUnsignedShort(InputStream stream, StreamDecoderState state, int defaultValue) throws DecodeException;

    Integer readInteger(InputStream stream, StreamDecoderState state) throws DecodeException;

    int readInteger(InputStream stream, StreamDecoderState state, int defaultValue) throws DecodeException;

    UnsignedInteger readUnsignedInteger(InputStream stream, StreamDecoderState state) throws DecodeException;

    int readUnsignedInteger(InputStream stream, StreamDecoderState state, int defaultValue) throws DecodeException;

    long readUnsignedInteger(InputStream stream, StreamDecoderState state, long defaultValue) throws DecodeException;

    Long readLong(InputStream stream, StreamDecoderState state) throws DecodeException;

    long readLong(InputStream stream, StreamDecoderState state, long defaultValue) throws DecodeException;

    UnsignedLong readUnsignedLong(InputStream stream, StreamDecoderState state) throws DecodeException;

    long readUnsignedLong(InputStream stream, StreamDecoderState state, long defaultValue) throws DecodeException;

    Float readFloat(InputStream stream, StreamDecoderState state) throws DecodeException;

    float readFloat(InputStream stream, StreamDecoderState state, float defaultValue) throws DecodeException;

    Double readDouble(InputStream stream, StreamDecoderState state) throws DecodeException;

    double readDouble(InputStream stream, StreamDecoderState state, double defaultValue) throws DecodeException;

    Binary readBinary(InputStream stream, StreamDecoderState state) throws DecodeException;

    ProtonBuffer readBinaryAsBuffer(InputStream stream, StreamDecoderState state) throws DecodeException;

    /**
     * This method expects to read a {@link Binary} encoded type from the provided buffer and
     * constructs a {@link DeliveryTag} type that wraps the bytes encoded.  If the encoding is
     * a NULL AMQP type then this method returns <code>null</code>.
     *
     * @param stream
     *      The {@link InputStream} to read a Binary encoded value from
     * @param state
     *      The current encoding state.
     *
     * @return a new DeliveryTag instance or null if an AMQP NULL encoding is found.
     *
     * @throws DecodeException if an error occurs while decoding the {@link DeliveryTag} instance.
     */
    DeliveryTag readDeliveryTag(InputStream stream, StreamDecoderState state) throws DecodeException;

    String readString(InputStream stream, StreamDecoderState state) throws DecodeException;

    Symbol readSymbol(InputStream stream, StreamDecoderState state) throws DecodeException;

    String readSymbol(InputStream stream, StreamDecoderState state, String defaultValue) throws DecodeException;

    Long readTimestamp(InputStream stream, StreamDecoderState state) throws DecodeException;

    long readTimestamp(InputStream stream, StreamDecoderState state, long defaultValue) throws DecodeException;

    UUID readUUID(InputStream stream, StreamDecoderState state) throws DecodeException;

    Object readObject(InputStream stream, StreamDecoderState state) throws DecodeException;

    <T> T readObject(InputStream stream, StreamDecoderState state, final Class<T> clazz) throws DecodeException;

    <T> T[] readMultiple(InputStream stream, StreamDecoderState state, final Class<T> clazz) throws DecodeException;

    <K,V> Map<K, V> readMap(InputStream stream, StreamDecoderState state) throws DecodeException;

    <V> List<V> readList(InputStream stream, StreamDecoderState state) throws DecodeException;

    /**
     * Reads from the given {@link InputStream} instance and returns a {@link StreamTypeDecoder} that can
     * read the next encoded AMQP type from the stream's bytes.  If an error occurs attempting to read
     * and determine the next type decoder an {@link DecodeException} is thrown.
     *
     * @param stream
     * 		The stream to read from to determine the next {@link TypeDecoder} needed.
     * @param state
     *      The {@link DecoderState} value that can be used for intermediate decoding tasks.
     *
     * @return a {@link StreamTypeDecoder} instance that can read the next type in the buffer.
     *
     * @throws DecodeException if an error occurs while reading the next type decoder.
     */
    StreamTypeDecoder<?> readNextTypeDecoder(InputStream stream, StreamDecoderState state) throws DecodeException;

    /**
     * Peeks ahead in the given {@link InputStream} instance and returns a {@link TypeDecoder} that can
     * read the next encoded AMQP type from the stream's bytes.  If an error occurs attempting to read
     * and determine the next type decoder an {@link DecodeException} is thrown.  The underlying stream
     * is not modified as a result of the peek operation and the returned {@link TypeDecoder} will fail
     * to properly read the type until the encoding bytes are read.  If the provided stream does not offer
     * support for the mark API than this method can throw an {@link UnsupportedOperationException}.
     *
     * @param stream
     * 		The stream to read from to determine the next {@link TypeDecoder} needed.
     * @param state
     *      The {@link DecoderState} value that can be used for intermediate decoding tasks.
     *
     * @return a {@link TypeDecoder} instance that can provide insight into the next type in the stream.
     *
     * @throws DecodeException if an error occurs while peeking ahead for the next type decoder.
     */
    StreamTypeDecoder<?> peekNextTypeDecoder(InputStream stream, StreamDecoderState state) throws DecodeException;

    /**
     * Allows custom {@link StreamDescribedTypeDecoder} instances to be registered with this {@link StreamDecoder}
     * which will be used if the described type encoding is encountered during decode operations.
     *
     * @param <V> The type that the decoder reads.
     *
     * @param decoder
     * 		A {@link StreamDescribedTypeDecoder} instance to be registered with this {@link StreamDecoder}
     *
     * @return this {@link StreamDecoder} instance.
     */
    <V> StreamDecoder registerDescribedTypeDecoder(StreamDescribedTypeDecoder<V> decoder);

}
