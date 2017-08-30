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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveTypeEncoder;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Symbol type values to a byte stream.
 */
public class SymbolTypeEncoder implements PrimitiveTypeEncoder<Symbol> {

    @Override
    public Class<Symbol> getTypeClass() {
        return Symbol.class;
    }

    @Override
    public void writeType(ByteBuf buffer, EncoderState state, Symbol value) {
        write(buffer, state, value, true);
    }

    @Override
    public void writeValue(ByteBuf buffer, EncoderState state, Symbol value) {
        write(buffer, state, value, false);
    }

    protected void write(ByteBuf buffer, EncoderState state, Symbol value, boolean writeEncoding) {
        byte[] symbolBytes = value.getBytes();

        if (symbolBytes.length <= 255) {
            if (writeEncoding) {
                buffer.writeByte(EncodingCodes.SYM8);
            }
            buffer.writeByte(symbolBytes.length);
            buffer.writeBytes(symbolBytes);
        } else {
            if (writeEncoding) {
                buffer.writeByte(EncodingCodes.SYM32);
            }
            buffer.writeInt(symbolBytes.length);
            buffer.writeBytes(symbolBytes);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Symbol[] values) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        //
        // Symbol types are variable sized values so we write the payload
        // and then we write the size using the result.
        int startIndex = buffer.writerIndex();

        // Reserve space for the size
        buffer.writeInt(0);

        buffer.writeInt(values.length);
        buffer.writeByte(EncodingCodes.SYM32);
        for (Symbol value : values) {
            byte[] symbolByte = value.getBytes();
            buffer.writeInt(symbolByte.length);
            buffer.writeBytes(symbolByte);
        }

        // Move back and write the size
        int endIndex = buffer.writerIndex();

        long size = endIndex - startIndex;
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given Symbol array, encoded size to large: " + size);
        }

        buffer.setInt(startIndex, (int) size);
    }
}
