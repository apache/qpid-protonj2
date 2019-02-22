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
package org.apache.qpid.proton4j.codec.encoders.transport;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.Released;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.encoders.AbstractDescribedListTypeEncoder;

/**
 * Encoder of AMQP Disposition type values to a byte stream
 */
public class DispositionTypeEncoder extends AbstractDescribedListTypeEncoder<Disposition> {

    @Override
    public UnsignedLong getDescriptorCode() {
        return Disposition.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Disposition.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Disposition> getTypeClass() {
        return Disposition.class;
    }

    @Override
    public void writeElement(Disposition disposition, int index, ProtonBuffer buffer, EncoderState state) {
        switch (index) {
            case 0:
                if (disposition.hasRole()) {
                    state.getEncoder().writeBoolean(buffer, state, disposition.getRole().getValue());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 1:
                if (disposition.hasFirst()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, disposition.getFirst());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 2:
                if (disposition.hasLast()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, disposition.getLast());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 3:
                if (disposition.hasSettled()) {
                    state.getEncoder().writeBoolean(buffer, state, disposition.getSettled());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 4:
                if (disposition.hasState()) {
                    if (disposition.getState() == Accepted.getInstance()) {
                        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
                        buffer.writeByte(EncodingCodes.SMALLULONG);
                        buffer.writeByte(Accepted.DESCRIPTOR_CODE.byteValue());
                        buffer.writeByte(EncodingCodes.LIST0);
                    } else {
                        state.getEncoder().writeObject(buffer, state, disposition.getState());
                    }
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }

                break;
            case 5:
                if (disposition.hasBatchable()) {
                    state.getEncoder().writeBoolean(buffer, state, disposition.getBatchable());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown Disposition value index: " + index);
        }
    }

    @Override
    public int getListEncoding(Disposition value) {
        if (value.getState() == null) {
            return EncodingCodes.LIST8;
        } else if (value.getState() == Accepted.getInstance() || value.getState() == Released.getInstance()) {
            return EncodingCodes.LIST8;
        } else {
            return EncodingCodes.LIST32;
        }
    }

    @Override
    public int getElementCount(Disposition disposition) {
        return disposition.getElementCount();
    }
}
