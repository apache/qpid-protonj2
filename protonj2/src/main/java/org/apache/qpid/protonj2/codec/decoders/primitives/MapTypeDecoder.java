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
import java.util.Map;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.decoders.PrimitiveTypeDecoder;

/**
 * Base interface for all AMQP Map type value decoders.
 */
@SuppressWarnings("rawtypes")
public interface MapTypeDecoder extends PrimitiveTypeDecoder<Map> {

    @Override
    default Class<Map> getTypeClass() {
        return Map.class;
    }

    /**
     * Reads the encoded size of the underlying Map type.
     *
     * @param buffer
     *      The buffer containing the encoded Map type.
     *
     * @return the size in bytes of the encoded Map.
     *
     * @throws DecodeException if an error occurs reading the value
     */
    int readSize(ProtonBuffer buffer) throws DecodeException;

    /**
     * Reads the count of entries in the encoded Map.
     * <p>
     * This value is the total count of all key values pairs, and should
     * always be an even number as Map types cannot be unbalanced.
     *
     * @param buffer
     *      The buffer containing the encoded Map type.
     *
     * @return the number of elements that we encoded from the original Map.
     *
     * @throws DecodeException if an error occurs reading the value
     */
    int readCount(ProtonBuffer buffer) throws DecodeException;

    /**
     * Reads the encoded size of the underlying Map type.
     *
     * @param stream
     *      The InputStream containing the encoded Map type.
     *
     * @return the size in bytes of the encoded Map.
     *
     * @throws DecodeException if an error occurs reading the value
     */
    int readSize(InputStream stream) throws DecodeException;

    /**
     * Reads the count of entries in the encoded Map.
     * <p>
     * This value is the total count of all key values pairs, and should
     * always be an even number as Map types cannot be unbalanced.
     *
     * @param stream
     *      The InputStream containing the encoded Map type.
     *
     * @return the number of elements that we encoded from the original Map.
     *
     * @throws DecodeException if an error occurs reading the value
     */
    int readCount(InputStream stream) throws DecodeException;

}