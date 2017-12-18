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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.decoders.AbstractPrimitiveTypeDecoder;

/**
 * Base class for the Symbol decoders used on AMQP Symbol types.
 */
public abstract class AbstractSymbolTypeDecoder extends AbstractPrimitiveTypeDecoder<Symbol> implements SymbolTypeDecoder {

    @Override
    public Symbol readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        int length = readSize(buffer);

        // TODO - While not optimal the symbol values are usually small in size but we
        //        should investigate if having a buffer slice method which lets us create
        //        a view of the buffer without copying it and then just skipping the bytes
        //        would be faster since we should eventually not be creating a new symbol
        //        from the bytes since we cache them internally in Symbol.
        ProtonBuffer symbolBuffer = ProtonByteBufferAllocator.DEFAULT.allocate(length, length);
        buffer.readBytes(symbolBuffer, length);

        return Symbol.getSymbol(symbolBuffer);
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        buffer.skipBytes(readSize(buffer));
    }

    protected abstract int readSize(ProtonBuffer buffer);

}
