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
package org.apache.qpid.protonj2.codec.encoders.transactions;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transactions.Discharge;

/**
 * Encoder of AMQP Discharge type values to a byte stream.
 */
public final class DischargeTypeEncoder extends AbstractDescribedListTypeEncoder<Discharge> {

    @Override
    public UnsignedLong getDescriptorCode() {
        return Discharge.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Discharge.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Discharge> getTypeClass() {
        return Discharge.class;
    }

    @Override
    public void writeElement(Discharge discharge, int index, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        switch (index) {
            case 0:
                encoder.writeBinary(buffer, state, discharge.getTxnId());
                break;
            case 1:
                buffer.writeByte(discharge.getFail() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                break;
            default:
                throw new IllegalArgumentException("Unknown Discharge value index: " + index);
        }
    }

    @Override
    public byte getListEncoding(Discharge value) {
        if (value.getTxnId() != null && value.getTxnId().getLength() > 240) {
            return EncodingCodes.LIST32;
        } else {
            return EncodingCodes.LIST8;
        }
    }

    @Override
    public int getElementCount(Discharge discharge) {
        return 2;
    }

    @Override
    public int getMinElementCount() {
        return 1;
    }
}
