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
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.encoders.AbstractDescribedListTypeEncoder;

/**
 * Encoder of AMQP Transfer type values to a byte stream.
 */
public class TransferTypeEncoder extends AbstractDescribedListTypeEncoder<Transfer> {

    @Override
    public UnsignedLong getDescriptorCode() {
        return Transfer.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Transfer.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Transfer> getTypeClass() {
        return Transfer.class;
    }

    @Override
    public void writeElement(Transfer transfer, int index, ProtonBuffer buffer, EncoderState state) {
        switch (index) {
            case 0:
                if (transfer.hasHandle()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, transfer.getHandle());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 1:
                if (transfer.hasDeliveryId()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, transfer.getDeliveryId());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 2:
                if (transfer.hasDeliveryTag()) {
                    state.getEncoder().writeDeliveryTag(buffer, state, transfer.getDeliveryTag());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 3:
                if (transfer.hasMessageFormat()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, transfer.getMessageFormat());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 4:
                if (transfer.hasSettled()) {
                    buffer.writeByte(transfer.getSettled() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 5:
                if (transfer.hasMore()) {
                    buffer.writeByte(transfer.getMore() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 6:
                if (transfer.hasRcvSettleMode()) {
                    state.getEncoder().writeUnsignedByte(buffer, state, transfer.getRcvSettleMode().byteValue());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 7:
                state.getEncoder().writeObject(buffer, state, transfer.getState());
                break;
            case 8:
                if (transfer.hasResume()) {
                    buffer.writeByte(transfer.getResume() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 9:
                if (transfer.hasAborted()) {
                    buffer.writeByte(transfer.getAborted() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 10:
                if (transfer.hasBatchable()) {
                    buffer.writeByte(transfer.getBatchable() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown Transfer value index: " + index);
        }
    }

    @Override
    public int getListEncoding(Transfer value) {
        if (value.getState() != null) {
            return EncodingCodes.LIST32;
        } else if (value.getDeliveryTag() != null && value.getDeliveryTag().tagLength() > 200) {
            return EncodingCodes.LIST32;
        } else {
            return EncodingCodes.LIST8;
        }
    }

    @Override
    public int getElementCount(Transfer transfer) {
        return transfer.getElementCount();
    }

    // TODO - Possible correctness checking

//    @Override
//    public int getMinElementCount() {
//        return 1;  // Handle required
//    }
}
