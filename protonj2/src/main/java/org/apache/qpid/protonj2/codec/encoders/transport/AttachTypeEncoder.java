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
import org.apache.qpid.protonj2.types.transport.Attach;

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
    public void writeElement(Attach attach, int index, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        if (attach.hasElement(index)) {
            switch (index) {
                case 0:
                    encoder.writeString(buffer, state, attach.getName());
                    break;
                case 1:
                    encoder.writeUnsignedInteger(buffer, state, attach.getHandle());
                    break;
                case 2:
                    buffer.writeByte(attach.getRole().encodingCode());
                    break;
                case 3:
                    encoder.writeUnsignedByte(buffer, state, attach.getSenderSettleMode().byteValue());
                    break;
                case 4:
                    encoder.writeUnsignedByte(buffer, state, attach.getReceiverSettleMode().byteValue());
                    break;
                case 5:
                    encoder.writeObject(buffer, state, attach.getSource());
                    break;
                case 6:
                    encoder.writeObject(buffer, state, attach.getTarget());
                    break;
                case 7:
                    encoder.writeMap(buffer, state, attach.getUnsettled());
                    break;
                case 8:
                    buffer.writeByte(attach.getIncompleteUnsettled() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                    break;
                case 9:
                    encoder.writeUnsignedInteger(buffer, state, attach.getInitialDeliveryCount());
                    break;
                case 10:
                    encoder.writeUnsignedLong(buffer, state, attach.getMaxMessageSize());
                    break;
                case 11:
                    encoder.writeArray(buffer, state, attach.getOfferedCapabilities());
                    break;
                case 12:
                    encoder.writeArray(buffer, state, attach.getDesiredCapabilities());
                    break;
                case 13:
                    encoder.writeMap(buffer, state, attach.getProperties());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown Attach value index: " + index);
            }
        } else {
            buffer.writeByte(EncodingCodes.NULL);
        }
    }

    @Override
    public byte getListEncoding(Attach value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getElementCount(Attach attach) {
        return attach.getElementCount();
    }

    @Override
    public int getMinElementCount() {
        return 3;
    }
}
