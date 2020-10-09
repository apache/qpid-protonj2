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

/**
 * Retains state of the {@link InputStream} based decode either between calls or across decode iterations
 */
public interface StreamDecoderState {

    /**
     * Resets any intermediate state back to default values.
     *
     * @return this {@link StreamDecoderState} instance.
     */
    StreamDecoderState reset();

    /**
     * @return the {@link StreamDecoder} that created this state object
     */
    StreamDecoder getDecoder();

    /**
     * Given a stream that will provide UTF-8 encoded bytes, decode and return the String that
     * represents that UTF-8 value.
     *
     * @param stream
     *      A stream from which the UTF-8 encoded bytes are to be decoded.
     * @param length
     *      The number of bytes in the passed {@link InputStream} that comprise the UTF-8 encoding.
     *
     * @return a String that represents the UTF-8 decoded bytes.
     *
     * @throws DecodeException if an error occurs while decoding the string value.
     */
    String decodeUTF8(InputStream stream, int length) throws DecodeException;

}
