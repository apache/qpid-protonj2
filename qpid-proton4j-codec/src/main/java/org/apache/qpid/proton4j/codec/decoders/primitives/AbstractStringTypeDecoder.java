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
package org.apache.qpid.proton4j.codec.decoders.primitives;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.proton4j.codec.DecoderState;

import io.netty.buffer.ByteBuf;

/**
 * Base for the various String type Decoders used to read AMQP String values.
 */
public abstract class AbstractStringTypeDecoder implements StringTypeDecoder {

    private static final CharsetDecoder STRING_DECODER = StandardCharsets.UTF_8.newDecoder();

    @Override
    public String readValue(ByteBuf buffer, DecoderState state) throws IOException {
        int length = readSize(buffer);

        byte[] bytes = new byte[length];
        buffer.readBytes(bytes, 0, length);

        try {
            return STRING_DECODER.decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Cannot parse String");
        }
    }

    @Override
    public void skipValue(ByteBuf buffer, DecoderState state) throws IOException {
        buffer.skipBytes(readSize(buffer));
    }

    protected abstract int readSize(ByteBuf buffer);

}
