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
package org.apache.qpid.proton4j.codec.decoders.transport;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.transport.Close;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.DescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder.ListEntryHandler;

import io.netty.buffer.ByteBuf;

/**
 * Decoder of AMQP Close type values from a byte stream
 */
public class CloseTypeDecoder implements DescribedTypeDecoder<Close>, ListEntryHandler<Close> {

    @Override
    public Class<Close> getTypeClass() {
        return Close.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Close.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Close.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Close readValue(ByteBuf buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        ListTypeDecoder listDecoder = (ListTypeDecoder) decoder;
        Close close = new Close();

        listDecoder.readValue(buffer, state, this, close);

        return close;
    }

    @Override
    public void onListEntry(int index, Close close, ByteBuf buffer, DecoderState state) throws IOException {
        switch (index) {
            case 0:
                close.setError(state.getDecoder().readObject(buffer, state, ErrorCondition.class));
                break;
            default:
                throw new IllegalStateException("To many entries in Close encoding");
        }
    }
}
