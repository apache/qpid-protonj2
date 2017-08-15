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
package org.apache.qpid.proton4j.codec.decoders.messaging;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.DeleteOnNoLinks;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.DescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.EncodingCodes;

import io.netty.buffer.ByteBuf;

/**
 * Decoder of AMQP DeleteOnNoLinks type values from a byte stream
 */
public class DeleteOnNoLinksTypeDecoder implements DescribedTypeDecoder<DeleteOnNoLinks> {

    @Override
    public Class<DeleteOnNoLinks> getTypeClass() {
        return DeleteOnNoLinks.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return DeleteOnNoLinks.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DeleteOnNoLinks.DESCRIPTOR_SYMBOL;
    }

    @Override
    public DeleteOnNoLinks readValue(ByteBuf buffer, DecoderState state) throws IOException {
        byte code = buffer.readByte();

        if (code != EncodingCodes.LIST0) {
            throw new IOException("Expected List0 type indicator but got code for type: " + code);
        }

        return DeleteOnNoLinks.getInstance();
    }

    @Override
    public void skipValue(ByteBuf buffer, DecoderState state) throws IOException {
        buffer.skipBytes(Byte.BYTES);
    }
}
