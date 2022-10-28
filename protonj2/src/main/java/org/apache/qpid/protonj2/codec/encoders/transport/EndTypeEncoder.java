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
package org.apache.qpid.protonj2.codec.encoders.transport;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.End;

/**
 * Encoder of AMQP End type values to a byte stream.
 */
public final class EndTypeEncoder extends AbstractDescribedListTypeEncoder<End> {

    @Override
    public UnsignedLong getDescriptorCode() {
        return End.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return End.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<End> getTypeClass() {
        return End.class;
    }

    @Override
    public void writeElement(End end, int index, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        switch (index) {
            case 0:
                encoder.writeObject(buffer, state, end.getError());
                break;
            default:
                throw new IllegalArgumentException("Unknown End value index: " + index);
        }
    }

    @Override
    public byte getListEncoding(End value) {
        return value.getError() == null ? EncodingCodes.LIST0 : EncodingCodes.LIST32;
    }

    @Override
    public int getElementCount(End end) {
        return end.getError() == null ? 0 : 1;
    }
}
