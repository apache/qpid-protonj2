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
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.encoders.AbstractDescribedListTypeEncoder;

/**
 * Encoder of AMQP Attach type values to a byte stream.
 */
public class AttachTypeEncoder extends AbstractDescribedListTypeEncoder<Attach> {

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
                state.getEncoder().writeString(buffer, state, attach.getName());
                break;
            case 1:
                state.getEncoder().writeUnsignedInteger(buffer, state, attach.getHandle());
                break;
            case 2:
                state.getEncoder().writeBoolean(buffer, state, attach.getRole().getValue());
                break;
            case 3:
                state.getEncoder().writeUnsignedByte(buffer, state, attach.getSndSettleMode().getValue());
                break;
            case 4:
                state.getEncoder().writeUnsignedByte(buffer, state, attach.getRcvSettleMode().getValue());
                break;
            case 5:
                state.getEncoder().writeObject(buffer, state, attach.getSource());
                break;
            case 6:
                state.getEncoder().writeObject(buffer, state, attach.getTarget());
                break;
            case 7:
                state.getEncoder().writeMap(buffer, state, attach.getUnsettled());
                break;
            case 8:
                state.getEncoder().writeBoolean(buffer, state, attach.getIncompleteUnsettled());
                break;
            case 9:
                state.getEncoder().writeUnsignedInteger(buffer, state, attach.getInitialDeliveryCount());
                break;
            case 10:
                state.getEncoder().writeUnsignedLong(buffer, state, attach.getMaxMessageSize());
                break;
            case 11:
                state.getEncoder().writeArray(buffer, state, attach.getOfferedCapabilities());
                break;
            case 12:
                state.getEncoder().writeArray(buffer, state, attach.getDesiredCapabilities());
                break;
            case 13:
                state.getEncoder().writeMap(buffer, state, attach.getProperties());
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
        if (attach.getProperties() != null) {
            return 14;
        } else if (attach.getDesiredCapabilities() != null) {
            return 13;
        } else if (attach.getOfferedCapabilities() != null) {
            return 12;
        } else if (attach.getMaxMessageSize() != null) {
            return 11;
        } else if (attach.getInitialDeliveryCount() != null) {
            return 10;
        } else if (attach.getIncompleteUnsettled()) {
            return 9;
        } else if (attach.getUnsettled() != null) {
            return 8;
        } else if (attach.getTarget() != null) {
            return 7;
        } else if (attach.getSource() != null) {
            return 6;
        } else if (attach.getRcvSettleMode() != null && !attach.getRcvSettleMode().equals(ReceiverSettleMode.FIRST)) {
            return 5;
        } else if (attach.getSndSettleMode() != null && !attach.getSndSettleMode().equals(SenderSettleMode.MIXED)) {
            return 4;
        } else {
            return 3;
        }
    }
}
