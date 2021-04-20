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

import java.io.IOException;
import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.decoders.AbstractPrimitiveTypeDecoder;

/**
 * Base for the various String type Decoders used to read AMQP String values.
 */
public abstract class AbstractStringTypeDecoder extends AbstractPrimitiveTypeDecoder<String> implements StringTypeDecoder {

    @Override
    public String readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final int length = readSize(buffer);

        if (length > buffer.getReadableBytes()) {
            throw new DecodeException(String.format(
                    "String encoded size %d is specified to be greater than the amount " +
                    "of data available (%d)", length, buffer.getReadableBytes()));
        }

        if (length != 0) {
            return state.decodeUTF8(buffer, length);
        } else {
            return "";
        }
    }

    @Override
    public String readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        final int length = readSize(stream);

        if (length != 0) {
            return state.decodeUTF8(stream, length);
        } else {
            return "";
        }
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        buffer.skipBytes(readSize(buffer));
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        try {
            stream.skip(readSize(stream));
        } catch (IOException ex) {
            throw new DecodeException("Error while reading String payload bytes", ex);
        }
    }

    protected abstract int readSize(ProtonBuffer buffer);

    protected abstract int readSize(InputStream stream);

}
