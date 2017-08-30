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
package org.apache.qpid.proton4j.codec.encoders.primitives;

import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveTypeEncoder;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Character type values to a byte stream.
 */
public class CharacterTypeEncoder implements PrimitiveTypeEncoder<Character> {

    @Override
    public Class<Character> getTypeClass() {
        return Character.class;
    }

    @Override
    public void writeType(ByteBuf buffer, EncoderState state, Character value) {
        buffer.writeByte(EncodingCodes.CHAR);
        buffer.writeInt(value.charValue() & 0xffff);
    }

    @Override
    public void writeValue(ByteBuf buffer, EncoderState state, Character value) {
        buffer.writeInt(value.charValue() & 0xffff);
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Character[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Integer.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given char array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.CHAR);
        for (Character charValue : value) {
            buffer.writeInt(charValue.charValue() & 0xffff);
        }
    }

    public void writeArray(ByteBuf buffer, EncoderState state, char[] value) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Integer.BYTES * value.length) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given char array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(value.length);
        buffer.writeByte(EncodingCodes.CHAR);
        for (char charValue : value) {
            buffer.writeInt(charValue & 0xffff);
        }
    }
}
