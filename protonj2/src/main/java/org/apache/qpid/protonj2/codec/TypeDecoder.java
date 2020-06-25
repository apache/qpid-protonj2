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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;

/**
 * Interface for an decoder of a specific AMQP Type.
 *
 * @param <V> The type that will be returned when this decoder reads a value.
 */
public interface TypeDecoder<V> {

    /**
     * @return the Class that this decoder handles.
     */
    Class<V> getTypeClass();

    /**
     * @return true if the underlying type that is going to be decoded is an array type
     */
    boolean isArrayType();

    /**
     * Reads the next type from the given buffer and returns it.
     *
     * @param buffer
     * 		the source of encoded data.
     * @param state
     * 		the current state of the decoder.
     *
     * @return the next instance in the stream that this decoder handles.
     *
     * @throws DecodeException if an error is encountered while reading the next value.
     */
    V readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Skips over the bytes that compose the type this descriptor decodes.
     * <p>
     * Skipping values can be used when the type is not used or processed by the
     * application doing the decoding.  An example might be an AMQP message decoder
     * that only needs to decode certain parts of the message and not others.
     *
     * @param buffer
     *      The buffer that contains the encoded type.
     * @param state
     *      The decoder state.
     *
     * @throws DecodeException if an error occurs while skipping the value.
     */
    void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads a series of this type that have been encoded into the body of an Array type.
     * <p>
     * When encoded into an array the values are encoded in series following the identifier
     * for the type, this method is given a count of the number of instances that are encoded
     * and should read each in succession and returning them in a new array.
     *
     * @param buffer
     *      the source of encoded data.
     * @param state
     *      the current state of the decoder.
     * @param count
     *      the number of array elements encoded in the buffer.
     *
     * @return the next instance in the stream that this decoder handles.
     *
     * @throws DecodeException if an error is encountered while reading the next value.
     */
    V[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException;

}
