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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractPrimitiveTypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;

/**
 * Base class for the Symbol decoders used on AMQP Symbol types.
 */
public abstract class AbstractSymbolTypeDecoder extends AbstractPrimitiveTypeDecoder<Symbol> implements SymbolTypeDecoder {

    @Override
    public Symbol readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        int length = readSize(buffer);

        if (length == 0) {
            return Symbol.valueOf("");
        }

        if (length > buffer.getReadableBytes()) {
            throw new DecodeException(String.format(
                    "Symbol encoded size %d is specified to be greater than the amount " +
                    "of data available (%d)", length, buffer.getReadableBytes()));
        }

        ProtonBuffer symbolBuffer = buffer.slice(buffer.getReadIndex(), length);
        buffer.skipBytes(length);

        return Symbol.getSymbol(symbolBuffer, true);
    }

    /**
     * Reads a String view of an encoded Symbol value from the given buffer.
     * <p>
     * This method has the same result as calling the Symbol reading variant
     * {@link #readValue(ProtonBuffer, DecoderState)} and then invoking the toString
     * method on the resulting Symbol.
     *
     * @param buffer
     *      The buffer to read the encoded symbol from.
     * @param state
     *      The encoder state that applied to this decode operation.
     *
     * @return a String view of the encoded Symbol value.
     *
     * @throws DecodeException if an error occurs decoding the Symbol from the given buffer.
     */
    public String readString(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        int length = readSize(buffer);

        if (length == 0) {
            return "";
        }

        ProtonBuffer symbolBuffer = buffer.slice(buffer.getReadIndex(), length);
        buffer.skipBytes(length);

        return Symbol.getSymbol(symbolBuffer, true).toString();
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        buffer.skipBytes(readSize(buffer));
    }

    /**
     * Subclasses must read the correct number of bytes from the buffer to determine the
     * size of the encoded Symbol value.
     *
     * @param buffer
     *      The buffer to read the size from.
     *
     * @return the number of bytes that make up the encoded Symbol value.
     *
     * @throws DecodeException if an error occurs reading the size value.
     */
    protected abstract int readSize(ProtonBuffer buffer) throws DecodeException;

}
