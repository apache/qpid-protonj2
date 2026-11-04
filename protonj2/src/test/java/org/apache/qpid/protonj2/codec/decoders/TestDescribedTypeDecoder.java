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
package org.apache.qpid.protonj2.codec.decoders;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;

public final class TestDescribedTypeDecoder extends AbstractDescribedTypeDecoder<String> {

    private final Symbol symbolDescriptor;
    private final UnsignedLong ulongDescriptor;

    public TestDescribedTypeDecoder(Symbol symbol, UnsignedLong ulong) {
        this.symbolDescriptor = symbol;
        this.ulongDescriptor = ulong;
    }

    @Override
    public String readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] readArrayElements(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readSize(ProtonBuffer buffer, DecoderState state) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean isArrayType() {
        return false;
    }

    @Override
    public Class<String> getTypeClass() {
        return String.class;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return symbolDescriptor;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return ulongDescriptor;
    }
}