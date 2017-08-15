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
import org.apache.qpid.proton4j.amqp.messaging.DeleteOnNoMessages;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.DescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.EncodingCodes;

import io.netty.buffer.ByteBuf;

/**
 * Decoder of AMQP DeleteOnNoLinks type values from a byte stream
 */
public class DeleteOnNoMessagesTypeDecoder implements DescribedTypeDecoder<DeleteOnNoMessages> {

    @Override
    public Class<DeleteOnNoMessages> getTypeClass() {
        return DeleteOnNoMessages.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return DeleteOnNoMessages.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return DeleteOnNoMessages.DESCRIPTOR_SYMBOL;
    }

    @Override
    public DeleteOnNoMessages readValue(ByteBuf buffer, DecoderState state) throws IOException {
        byte code = buffer.readByte();

        if (code != EncodingCodes.LIST0) {
            throw new IOException("Expected List0 type indicator but got code for type: " + code);
        }

        return DeleteOnNoMessages.getInstance();
    }

    @Override
    public void skipValue(ByteBuf buffer, DecoderState state) throws IOException {
        buffer.skipBytes(Byte.BYTES);
    }
}
