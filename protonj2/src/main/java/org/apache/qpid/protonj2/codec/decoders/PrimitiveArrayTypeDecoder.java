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

}
