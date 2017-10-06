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
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DescribedListTypeEncoder;
import org.apache.qpid.proton4j.codec.EncoderState;

/**
 * Encoder of AMQP Flow type values to a byte stream.
 */
public class FlowTypeEncoder implements DescribedListTypeEncoder<Flow> {

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
                state.getEncoder().writeUnsignedInteger(buffer, state, flow.getNextIncomingId());
                break;
            case 1:
                state.getEncoder().writeUnsignedInteger(buffer, state, flow.getIncomingWindow());
                break;
            case 2:
                state.getEncoder().writeUnsignedInteger(buffer, state, flow.getNextOutgoingId());
                break;
            case 3:
                state.getEncoder().writeUnsignedInteger(buffer, state, flow.getOutgoingWindow());
                break;
            case 4:
                state.getEncoder().writeUnsignedInteger(buffer, state, flow.getHandle());
                break;
            case 5:
                state.getEncoder().writeUnsignedInteger(buffer, state, flow.getDeliveryCount());
                break;
            case 6:
                state.getEncoder().writeUnsignedInteger(buffer, state, flow.getLinkCredit());
                break;
            case 7:
                state.getEncoder().writeUnsignedInteger(buffer, state, flow.getAvailable());
                break;
            case 8:
                state.getEncoder().writeBoolean(buffer, state, flow.getDrain());
                break;
            case 9:
                state.getEncoder().writeBoolean(buffer, state, flow.getEcho());
                break;
            case 10:
                state.getEncoder().writeMap(buffer, state, flow.getProperties());
                break;
            default:
                throw new IllegalArgumentException("Unknown Flow value index: " + index);
        }
    }

    @Override
    public int getElementCount(Flow flow) {
        if (flow.getProperties() != null) {
            return 11;
        } else if (flow.getEcho()) {
            return 10;
        } else if (flow.getDrain()) {
            return 9;
        } else if (flow.getAvailable() != null) {
            return 8;
        } else if (flow.getLinkCredit() != null) {
            return 7;
        } else if (flow.getDeliveryCount() != null) {
            return 6;
        } else if (flow.getHandle() != null) {
            return 5;
        } else {
            return 4;
        }
    }
}
