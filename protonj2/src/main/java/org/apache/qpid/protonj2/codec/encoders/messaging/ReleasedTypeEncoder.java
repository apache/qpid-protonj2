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
package org.apache.qpid.protonj2.codec.encoders.messaging;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Released;

/**
 * Encoder of AMQP Released type values to a byte stream
 */
public final class ReleasedTypeEncoder extends AbstractDescribedListTypeEncoder<Released> {

    private static final byte[] RELEASED_ENCODING = new byte[] {
        EncodingCodes.DESCRIBED_TYPE_INDICATOR,
        EncodingCodes.SMALLULONG,
        Released.DESCRIPTOR_CODE.byteValue(),
        EncodingCodes.LIST0
    };

    @Override
    public Class<Released> getTypeClass() {
        return Released.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Released.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Released.DESCRIPTOR_SYMBOL;
    }

    @Override
    public byte getListEncoding(Released value) {
        return EncodingCodes.LIST0;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Released value) {
        buffer.writeBytes(RELEASED_ENCODING);
    }

    @Override
    public void writeElement(Released source, int index, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
    }

    @Override
    public int getElementCount(Released value) {
        return 0;
    }
}
