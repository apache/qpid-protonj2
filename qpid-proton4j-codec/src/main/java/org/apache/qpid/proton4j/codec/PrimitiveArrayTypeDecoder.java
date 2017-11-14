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

import java.io.IOException;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;

public interface PrimitiveArrayTypeDecoder extends PrimitiveTypeDecoder<Object[]> {

    @Override
    default Class<Object[]> getTypeClass() {
        return Object[].class;
    }

    @Override
    default boolean isArrayTypeDecoder() {
        return true;
    }

    @Override
    default Object[] readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        return readValueAsObjectArray(buffer, state);
    }

    /**
     * Reads the encoded Array and returns it as an opoque Object[] meaning
     * that any primitive language types (e.g. int, long, boolean, etc) are
     * return using an array of primitive type Objects (e.g. Integer, Long,
     * Boolean, etc).
     *
     * @param buffer
     * 		The buffer to read from.
     * @param state
     * 		The decoder state to use while decoding.
     *
     * @return an opaque Object[] that represents the underlying array.
     *
     * @throws IOException if an error occurs during the decode.
     */
    Object[] readValueAsObjectArray(ProtonBuffer buffer, DecoderState state) throws IOException;

    /**
     * Reads the encoded Array and returns it as an opaque Object rather
     * than an Object[] which allows for an array of java primitives to be
     * returned instead of an array of primitive type Objects.
     *
     * @param buffer
     * 		The buffer to read from.
     * @param state
     * 		The decoder state to use while decoding.
     *
     * @return an opaque object that represents the underlying array.
     *
     * @throws IOException if an error occurs during the decode.
     */
    Object readValueAsObject(ProtonBuffer buffer, DecoderState state) throws IOException;

}
