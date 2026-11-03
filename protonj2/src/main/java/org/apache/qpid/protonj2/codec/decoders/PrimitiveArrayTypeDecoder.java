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
package org.apache.qpid.protonj2.codec.decoders;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;

/**
 * Provides an interface for an Array type decoder that provides the Proton decoder
 * with entry points to read arrays in a manner that support the desired Java array
 * type to be returned.
 */
public interface PrimitiveArrayTypeDecoder extends PrimitiveTypeDecoder<Object> {

    /**
     * Reads the given array from the bytes in the buffer but only if the type encoding
     * of the given array matches the given {@link Class} filter. This allows the caller
     * to effectively limit if the given array is decoded at all if the type encoding is
     * not a match and also can act to limit an array to only a single level as the type
     * encoding of nested arrays will not be the value type until the bottom of the array
     * is reached. To allow all types and any array nesting the caller should pass the
     * {@link Object} class.
     *
     * @param buffer
     * 		the source of encoded data.
     * @param state
     * 		the current state of the decoder.
     * @param ofType
     *      the type encoding of the desired array.
     *
     * @return the next instance in the byte stream that this decoder handles.
     *
     * @throws DecodeException if an error is encountered while reading the next value.
     */
    Object readValue(ProtonBuffer buffer, DecoderState state, Class<?> ofType) throws DecodeException;

    /**
     * Reads the given array from the bytes in the buffer but only if the type encoding
     * of the given array matches the given {@link Class} filter. This allows the caller
     * to effectively limit if the given array is decoded at all if the type encoding is
     * not a match and also can act to limit an array to only a single level as the type
     * encoding of nested arrays will not be the value type until the bottom of the array
     * is reached. To allow all types and any array nesting the caller should pass the
     * {@link Object} class.
     *
     * @param stream
     * 		the source of encoded data.
     * @param state
     * 		the current state of the decoder.
     * @param ofType
     *      the type encoding of the desired array.
     *
     * @return the next instance in the stream that this decoder handles.
     *
     * @throws DecodeException if an error is encountered while reading the next value.
     */
    Object readValue(InputStream stream, StreamDecoderState state, Class<?> ofType) throws DecodeException;

    /**
     * Reads the number of elements in the encoded primitive array from the given buffer and
     * returns it. Since this methods advances the read position of the provided buffer the
     * caller must either reset that based on a previous mark or they must read the primitive
     * payload manually as the decoder would not be able to read the value as it has no retained
     * state.
     *
     * @param buffer
     * 		the source of encoded data.
     * @param state
     * 		the current state of the decoder.
     *
     * @return the size in bytes of the encoded primitive value.
     *
     * @throws DecodeException if an error is encountered while reading the encoded size.
     */
    int readCount(ProtonBuffer buffer, DecoderState state);

    /**
     * Reads the number of elements in the encoded primitive from the given {@link InputStream}
     * and returns it. Since this methods advances the read position of the provided stream the
     * caller must either reset that based on a previous mark or they must read the primitive
     * payload manually as the decoder would not be able to read the value as it has no
     * retained state.
     *
     * @param stream
     * 		the source of encoded data.
     * @param state
     * 		the current state of the decoder.
     *
     * @return the size in bytes of the encoded primitive value.
     *
     * @throws DecodeException if an error is encountered while reading the encoded size.
     */
    int readCount(InputStream stream, StreamDecoderState state);

    /**
     * Validates the basic requirements for the count field of an array encoding.
     *
     * @param count
     * 		The count value read from the byte source
     * @param buffer
     * 		The byte source that contains the remaining encoded bytes
     * @param state
     * 		The decoder state used during this decode operation.
     * @param decoder
     * 		The type encoder that is performing the current decode.
     *
     * @throws DecodeException if the count value violates the constraints.
     */
    static void validateArrayConstraints(int count, ProtonBuffer buffer, DecoderState state, PrimitiveTypeDecoder<?> decoder) throws DecodeException {
        if (decoder.isZeroWidth()) {
            if (Integer.compareUnsigned(count, state.getMaxZeroWidthArrayElements()) > 0) {
                throw new DecodeException(String.format(
                    "Encoded array count %d is specified to be greater than limit for zero sized encoded array types (%d)",
                    Integer.toUnsignedLong(count), state.getMaxZeroWidthArrayElements()));
            }
        } else if (Integer.compareUnsigned(count, buffer.getReadableBytes()) > 0) {
            throw new DecodeException(String.format(
                "Encoded array count %d is specified to be greater than the amount " +
                "of data available (%d)", Integer.toUnsignedLong(count), buffer.getReadableBytes()));
        }
    }

    /**
     * Validates the basic requirements for the count field of an array encoding.
     *
     * @param count
     * 		The count value read from the byte source
     * @param stream
     * 		The byte source that contains the remaining encoded bytes
     * @param state
     * 		The decoder state used during this decode operation.
     * @param decoder
     * 		The type encoder that is performing the current decode.
     *
     * @throws DecodeException if the count value violates the constraints.
     */
    static void validateArrayConstraints(int count, InputStream stream, StreamDecoderState state, PrimitiveTypeDecoder<?> decoder) throws DecodeException {
        if (decoder.isZeroWidth()) {
            if (Integer.compareUnsigned(count, state.getMaxZeroWidthArrayElements()) > 0) {
                throw new DecodeException(String.format(
                    "Encoded array count %d is specified to be greater than limit for zero sized encoded array types (%d)",
                    Integer.toUnsignedLong(count), state.getMaxZeroWidthArrayElements()));
            }
        } else if (Integer.compareUnsigned(count, state.getMaxArraySize()) > 0) {
            throw new DecodeException(String.format(
                    "Encoded array count %d is specified to be greater than the amount " +
                    "of the configured max array length (%d)", Integer.toUnsignedLong(count), state.getMaxArraySize()));
        }
    }
}
