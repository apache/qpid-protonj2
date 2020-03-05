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
import java.util.Collections;
import java.util.List;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.decoders.AbstractPrimitiveTypeDecoder;

/**
 * Decoder of Zero sized AMQP List values from a byte stream.
 */
@SuppressWarnings( { "unchecked", "rawtypes" } )
public final class List0TypeDecoder extends AbstractPrimitiveTypeDecoder<List> implements ListTypeDecoder {

    @Override
    public List<Object> readValue(ProtonBuffer buffer, DecoderState state) {
        return Collections.EMPTY_LIST;
    }

    @Override
    public int getTypeCode() {
        return EncodingCodes.LIST0 & 0xff;
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws IOException {
    }

    @Override
    public int readSize(ProtonBuffer buffer) {
        return 0;
    }

    @Override
    public int readCount(ProtonBuffer buffer) {
        return 0;
    }
}
