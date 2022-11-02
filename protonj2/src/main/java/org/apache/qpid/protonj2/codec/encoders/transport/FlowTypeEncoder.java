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

    /*
     * This assumes that the value was already check be the setter in Flow
     */
    private static void writeCheckedUnsignedInteger(final long value, final ProtonBuffer buffer) {
        if (value == 0) {
            buffer.writeByte(EncodingCodes.UINT0);
        } else if (value <= 255) {
            buffer.writeByte(EncodingCodes.SMALLUINT);
            buffer.writeByte((byte) value);
        } else {
            buffer.writeByte(EncodingCodes.UINT);
            buffer.writeInt((int) value);
        }
    }

    @Override
    public void writeElement(Flow flow, int index, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        if (flow.hasElement(index)) {
            switch (index) {
                case 0:
                    writeCheckedUnsignedInteger(flow.getNextIncomingId(), buffer);
                    break;
                case 1:
                    writeCheckedUnsignedInteger(flow.getIncomingWindow(), buffer);
                    break;
                case 2:
                    writeCheckedUnsignedInteger(flow.getNextOutgoingId(), buffer);
                    break;
                case 3:
                    writeCheckedUnsignedInteger(flow.getOutgoingWindow(), buffer);
                    break;
                case 4:
                    writeCheckedUnsignedInteger(flow.getHandle(), buffer);
                    break;
                case 5:
                    writeCheckedUnsignedInteger(flow.getDeliveryCount(), buffer);
                    break;
                case 6:
                    writeCheckedUnsignedInteger(flow.getLinkCredit(), buffer);
                    break;
                case 7:
                    writeCheckedUnsignedInteger(flow.getAvailable(), buffer);
                    break;
                case 8:
                    buffer.writeByte(flow.getDrain() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                    break;
                case 9:
                    buffer.writeByte(flow.getEcho() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                    break;
                case 10:
                    encoder.writeMap(buffer, state, flow.getProperties());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown Flow value index: " + index);
            }
        } else {
            buffer.writeByte(EncodingCodes.NULL);
        }
    }

    @Override
    public byte getListEncoding(Flow value) {
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

    @Override
    public int getMinElementCount() {
        return 4;
    }
}
