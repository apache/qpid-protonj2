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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.PrimitiveTypeDecoder;
import org.apache.qpid.protonj2.types.Binary;

/**
 * Base for all Binary type value decoders.
 */
public interface BinaryTypeDecoder extends PrimitiveTypeDecoder<Binary> {

    @Override
    default Class<Binary> getTypeClass() {
        return Binary.class;
    }

    /**
     * Reads the encoded size value for the encoded binary payload and returns it.  The
     * read is destructive and the {@link TypeDecoder} read methods cannot be called after
     * this unless the {@link ProtonBuffer} is reset via a position marker.  This method can
     * be useful when the caller intends to manually read the binary payload from the given
     * {@link ProtonBuffer}.
     *
     * @param buffer
     *      the buffer from which the binary encoded size should be read.
     *
     * @return the size of the binary payload that is encoded in the given {@link ProtonBuffer}.
     *
     * @throws DecodeException if an error occurs while reading the binary size.
     */
    int readSize(ProtonBuffer buffer) throws DecodeException;

    /**
     * Reads the encoded size value for the encoded binary payload and returns it.  The
     * read is destructive and the {@link TypeDecoder} read methods cannot be called after
     * this unless the {@link InputStream} is reset via a position marker.  This method can
     * be useful when the caller intends to manually read the binary payload from the given
     * {@link InputStream}.
     *
     * @param stream
     *      the stream from which the binary encoded size should be read.
     *
     * @return the size of the binary payload that is encoded in the given {@link InputStream}.
     *
     * @throws DecodeException if an error occurs while reading the binary size.
     */
    int readSize(InputStream stream) throws DecodeException;

}