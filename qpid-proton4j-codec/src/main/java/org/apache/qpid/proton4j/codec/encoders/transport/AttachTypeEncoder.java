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
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.encoders.AbstractDescribedListTypeEncoder;

/**
 * Encoder of AMQP Attach type values to a byte stream.
 */
public final class AttachTypeEncoder extends AbstractDescribedListTypeEncoder<Attach> {

    @Override
    public UnsignedLong getDescriptorCode() {
        return Attach.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Attach.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Attach> getTypeClass() {
        return Attach.class;
    }

    @Override
    public void writeElement(Attach attach, int index, ProtonBuffer buffer, EncoderState state) {
        switch (index) {
            case 0:
                if (attach.hasName()) {
                    state.getEncoder().writeString(buffer, state, attach.getName());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 1:
                if (attach.hasHandle()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, attach.getHandle());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 2:
                if (attach.hasRole()) {
                    buffer.writeByte(attach.getRole().getValue() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 3:
                if (attach.hasSenderSettleMode()) {
                    state.getEncoder().writeUnsignedByte(buffer, state, attach.getSenderSettleMode().byteValue());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 4:
                if (attach.hasReceiverSettleMode()) {
                    state.getEncoder().writeUnsignedByte(buffer, state, attach.getReceiverSettleMode().byteValue());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 5:
                if (attach.hasSource()) {
                    state.getEncoder().writeObject(buffer, state, attach.getSource());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 6:
                if (attach.hasTargetOrCoordinator()) {
                    state.getEncoder().writeObject(buffer, state, attach.getTargetOrCoordinator());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 7:
                if (attach.hasUnsettled()) {
                    state.getEncoder().writeMap(buffer, state, attach.getUnsettled());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 8:
                if (attach.hasIncompleteUnsettled()) {
                    buffer.writeByte(attach.getIncompleteUnsettled() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 9:
                if (attach.hasInitialDeliveryCount()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, attach.getInitialDeliveryCount());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 10:
                if (attach.hasMaxMessageSize()) {
                    state.getEncoder().writeUnsignedLong(buffer, state, attach.getMaxMessageSize());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 11:
                if (attach.hasOfferedCapabilites()) {
                    state.getEncoder().writeArray(buffer, state, attach.getOfferedCapabilities());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 12:
                if (attach.hasDesiredCapabilites()) {
                    state.getEncoder().writeArray(buffer, state, attach.getDesiredCapabilities());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 13:
                if (attach.hasProperties()) {
                    state.getEncoder().writeMap(buffer, state, attach.getProperties());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown Attach value index: " + index);
        }
    }

    @Override
    public int getListEncoding(Attach value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getElementCount(Attach attach) {
        return attach.getElementCount();
    }
}
