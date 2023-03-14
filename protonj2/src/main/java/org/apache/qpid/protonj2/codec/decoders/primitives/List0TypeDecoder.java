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
import java.util.Collections;
import java.util.List;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractPrimitiveTypeDecoder;

/**
 * Decoder of Zero sized AMQP List values from a byte stream.
 */
@SuppressWarnings( { "unchecked", "rawtypes" } )
public final class List0TypeDecoder extends AbstractPrimitiveTypeDecoder<List> implements ListTypeDecoder {

    @Override
    public int getTypeCode() {
        return EncodingCodes.LIST0 & 0xff;
    }

    @Override
    public List<Object> readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return Collections.EMPTY_LIST;
    }

    @Override
    public List<Object> readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        return Collections.EMPTY_LIST;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
    }

    @Override
    public int readSize(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return 0;
    }

    @Override
    public int readCount(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return 0;
    }

    @Override
    public int readSize(InputStream stream, StreamDecoderState state) throws DecodeException {
        return 0;
    }

    @Override
    public int readCount(InputStream stream, StreamDecoderState state) throws DecodeException {
        return 0;
    }
}
