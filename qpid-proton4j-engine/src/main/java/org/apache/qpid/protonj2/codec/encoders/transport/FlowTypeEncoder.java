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
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transport.Flow;

/**
 * Encoder of AMQP Flow type values to a byte stream.
 */
public final class FlowTypeEncoder extends AbstractDescribedListTypeEncoder<Flow> {

    @Override
    public UnsignedLong getDescriptorCode() {
        return Flow.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Flow.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Flow> getTypeClass() {
        return Flow.class;
    }

    @Override
    public void writeElement(Flow flow, int index, ProtonBuffer buffer, EncoderState state) {
        switch (index) {
            case 0:
                if (flow.hasNextIncomingId()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, flow.getNextIncomingId());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 1:
                if (flow.hasIncomingWindow()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, flow.getIncomingWindow());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 2:
                if (flow.hasNextOutgoingId()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, flow.getNextOutgoingId());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 3:
                if (flow.hasOutgoingWindow()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, flow.getOutgoingWindow());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 4:
                if (flow.hasHandle()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, flow.getHandle());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 5:
                if (flow.hasDeliveryCount()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, flow.getDeliveryCount());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 6:
                if (flow.hasLinkCredit()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, flow.getLinkCredit());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 7:
                if (flow.hasAvailable()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, flow.getAvailable());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 8:
                if (flow.hasDrain()) {
                    buffer.writeByte(flow.getDrain() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 9:
                if (flow.hasEcho()) {
                    buffer.writeByte(flow.getEcho() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 10:
                state.getEncoder().writeMap(buffer, state, flow.getProperties());
                break;
            default:
                throw new IllegalArgumentException("Unknown Flow value index: " + index);
        }
    }

    @Override
    public int getListEncoding(Flow value) {
        if (value.getProperties() == null) {
            return EncodingCodes.LIST8;
        } else {
            return EncodingCodes.LIST32;
        }
    }

    @Override
    public int getElementCount(Flow flow) {
        return flow.getElementCount();
    }
}
