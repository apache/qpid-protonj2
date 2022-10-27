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
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Accepted;

/**
 * Encoder of AMQP Accepted type values to a byte stream
 */
public final class AcceptedTypeEncoder extends AbstractDescribedListTypeEncoder<Accepted> {

    private static final byte[] ACCEPTED_ENCODING = new byte[] {
        EncodingCodes.DESCRIBED_TYPE_INDICATOR,
        EncodingCodes.SMALLULONG,
        Accepted.DESCRIPTOR_CODE.byteValue(),
        EncodingCodes.LIST0
    };

    @Override
    public Class<Accepted> getTypeClass() {
        return Accepted.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Accepted.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Accepted.DESCRIPTOR_SYMBOL;
    }

    @Override
    public byte getListEncoding(Accepted value) {
        return EncodingCodes.LIST0 & 0xff;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Accepted value) {
        buffer.writeBytes(ACCEPTED_ENCODING);
    }

    @Override
    public void writeElement(Accepted source, int index, ProtonBuffer buffer, EncoderState state) {
    }

    @Override
    public int getElementCount(Accepted value) {
        return 0;
    }
}
