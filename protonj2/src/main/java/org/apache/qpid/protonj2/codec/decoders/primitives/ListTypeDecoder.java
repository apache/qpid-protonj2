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
package org.apache.qpid.protonj2.codec.decoders.primitives;

import java.io.InputStream;
import java.util.List;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.PrimitiveTypeDecoder;

/**
 * Base class for List type decoders.
 */
@SuppressWarnings("rawtypes")
public interface ListTypeDecoder extends PrimitiveTypeDecoder<List> {

    @Override
    default Class<List> getTypeClass() {
        return List.class;
    }

    /**
     * Reads the number of elements contained in the encoded list from the provided {@link ProtonBuffer}.
     *
     * The implementation must read the correct size encoding based on the type of {@link List}
     * that this {@link TypeDecoder} handles.
     *
     * @param buffer
     * 		The buffer where the size value should be read from.
     * @param state
     *		The decoder state that is in used during this decode operation.
     *
     * @return an integer containing the number of elements in the encoded {@link List}.
     *
     * @throws DecodeException if an error occurs while reading the encoded element count.
     */
    int readCount(ProtonBuffer buffer, DecoderState state) throws DecodeException;

    /**
     * Reads the number of elements contained in the encoded list from the provided {@link InputStream}.
     *
     * The implementation must read the correct size encoding based on the type of {@link List}
     * that this {@link TypeDecoder} handles.
     *
     * @param stream
     * 		The stream where the size value should be read from.
     * @param state
     *		The decoder state that is in used during this decode operation.
     *
     * @return an integer containing the number of elements in the encoded {@link List}.
     *
     * @throws DecodeException if an error occurs while reading the encoded element count.
     */
    int readCount(InputStream stream, StreamDecoderState state) throws DecodeException;

}