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
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;
import org.apache.qpid.protonj2.types.Symbol;

/**
 * Decoder of AMQP Symbol values from a byte stream, this variant produces the singleton
 * Symbol values from the Symbol objects SASL Symbol cache as opposed to the main Symbol
 * separating Symbols only used during SASL exchanges from those used during the application
 * logic leaving more space for application Symbols in that cache.
 */
public final class SaslSymbol8TypeDecoder extends AbstractSymbolTypeDecoder {

    public static final SaslSymbol8TypeDecoder INSTANCE = new SaslSymbol8TypeDecoder();

    @Override
    public int getTypeCode() {
        return EncodingCodes.SYM8 & 0xff;
    }

    @Override
    public int readSize(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return buffer.readByte() & 0xff;
    }

    @Override
    public int readSize(InputStream stream, StreamDecoderState state) throws DecodeException {
        return ProtonStreamUtils.readByte(stream) & 0xff;
    }

    @Override
    protected Symbol getSymbol(ProtonBuffer buffer, boolean copyOnCreate) {
        return Symbol.getSASLSymbol(buffer, copyOnCreate);
    }
}
