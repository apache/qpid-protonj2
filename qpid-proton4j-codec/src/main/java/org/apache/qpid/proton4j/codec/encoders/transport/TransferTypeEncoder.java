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
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DescribedListTypeEncoder;
import org.apache.qpid.proton4j.codec.EncoderState;

/**
 * Encoder of AMQP Transfer type values to a byte stream.
 */
public class TransferTypeEncoder implements DescribedListTypeEncoder<Transfer> {

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
                state.getEncoder().writeUnsignedInteger(buffer, state, transfer.getHandle());
                break;
            case 1:
                state.getEncoder().writeUnsignedInteger(buffer, state, transfer.getDeliveryId());
                break;
            case 2:
                state.getEncoder().writeBinary(buffer, state, transfer.getDeliveryTag());
                break;
            case 3:
                state.getEncoder().writeUnsignedInteger(buffer, state, transfer.getMessageFormat());
                break;
            case 4:
                state.getEncoder().writeBoolean(buffer, state, transfer.getSettled());
                break;
            case 5:
                state.getEncoder().writeBoolean(buffer, state, transfer.getMore());
                break;
            case 6:
                ReceiverSettleMode rcvSettleMode = transfer.getRcvSettleMode();
                state.getEncoder().writeObject(buffer, state, rcvSettleMode == null ? null : rcvSettleMode.getValue());
                break;
            case 7:
                state.getEncoder().writeObject(buffer, state, transfer.getState());
                break;
            case 8:
                state.getEncoder().writeBoolean(buffer, state, transfer.getResume());
                break;
            case 9:
                state.getEncoder().writeBoolean(buffer, state, transfer.getAborted());
                break;
            case 10:
                state.getEncoder().writeBoolean(buffer, state, transfer.getBatchable());
                break;
            default:
                throw new IllegalArgumentException("Unknown Transfer value index: " + index);
        }
    }

    @Override
    public int getElementCount(Transfer transfer) {
        if (transfer.getBatchable()) {
            return 11;
        } else if (transfer.getAborted()) {
            return 10;
        } else if (transfer.getResume()) {
            return 9;
        } else if (transfer.getState() != null) {
            return 8;
        } else if (transfer.getRcvSettleMode() != null) {
            return 7;
        } else if (transfer.getMore()) {
            return 6;
        } else if (transfer.getSettled() != null) {
            return 5;
        } else if (transfer.getMessageFormat() != null) {
            return 4;
        } else if (transfer.getDeliveryTag() != null) {
            return 3;
        } else if (transfer.getDeliveryId() != null) {
            return 2;
        } else {
            return 1;
        }
    }
}
