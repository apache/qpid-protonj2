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
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.encoders.AbstractDescribedListTypeEncoder;

/**
 * Encoder of AMQP Begin type values to a byte stream.
 */
public class BeginTypeEncoder extends AbstractDescribedListTypeEncoder<Begin> {

    @Override
    public UnsignedLong getDescriptorCode() {
        return Begin.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Begin.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Begin> getTypeClass() {
        return Begin.class;
    }

    @Override
    public void writeElement(Begin begin, int index, ProtonBuffer buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeUnsignedShort(buffer, state, begin.getRemoteChannel());
                break;
            case 1:
                state.getEncoder().writeUnsignedInteger(buffer, state, begin.getNextOutgoingId());
                break;
            case 2:
                state.getEncoder().writeUnsignedInteger(buffer, state, begin.getIncomingWindow());
                break;
            case 3:
                state.getEncoder().writeUnsignedInteger(buffer, state, begin.getOutgoingWindow());
                break;
            case 4:
                state.getEncoder().writeUnsignedInteger(buffer, state, begin.getHandleMax());
                break;
            case 5:
                state.getEncoder().writeArray(buffer, state, begin.getOfferedCapabilities());
                break;
            case 6:
                state.getEncoder().writeArray(buffer, state, begin.getDesiredCapabilities());
                break;
            case 7:
                state.getEncoder().writeMap(buffer, state, begin.getProperties());
                break;
            default:
                throw new IllegalArgumentException("Unknown Begin value index: " + index);
        }
    }

    @Override
    public int getListEncoding(Begin value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getElementCount(Begin begin) {
        if (begin.getProperties() != null) {
            return 8;
        } else if (begin.getDesiredCapabilities() != null) {
            return 7;
        } else if (begin.getOfferedCapabilities() != null) {
            return 6;
        } else if (begin.getHandleMax() != null && !begin.getHandleMax().equals(UnsignedInteger.MAX_VALUE)) {
            return 5;
        } else {
            return 4;
        }
    }
}
