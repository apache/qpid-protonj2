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
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.encoders.DescribedListTypeEncoder;

/**
 * Encoder of AMQP Open type values to a byte stream.
 */
public class OpenTypeEncoder implements DescribedListTypeEncoder<Open> {

    @Override
    public UnsignedLong getDescriptorCode() {
        return Open.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Open.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Open> getTypeClass() {
        return Open.class;
    }

    @Override
    public void writeElement(Open open, int index, ProtonBuffer buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeString(buffer, state, open.getContainerId());
                break;
            case 1:
                state.getEncoder().writeString(buffer, state, open.getHostname());
                break;
            case 2:
                state.getEncoder().writeUnsignedInteger(buffer, state, open.getMaxFrameSize());
                break;
            case 3:
                state.getEncoder().writeUnsignedShort(buffer, state, open.getChannelMax());
                break;
            case 4:
                state.getEncoder().writeUnsignedInteger(buffer, state, open.getIdleTimeOut());
                break;
            case 5:
                state.getEncoder().writeArray(buffer, state, open.getOutgoingLocales());
                break;
            case 6:
                state.getEncoder().writeArray(buffer, state, open.getIncomingLocales());
                break;
            case 7:
                state.getEncoder().writeArray(buffer, state, open.getOfferedCapabilities());
                break;
            case 8:
                state.getEncoder().writeArray(buffer, state, open.getDesiredCapabilities());
                break;
            case 9:
                state.getEncoder().writeMap(buffer, state, open.getProperties());
                break;
            default:
                throw new IllegalArgumentException("Unknown Open value index: " + index);
        }
    }

    @Override
    public int getListEncoding(Open value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getElementCount(Open open) {
        if (open.getProperties() != null) {
            return 10;
        } else if (open.getDesiredCapabilities() != null) {
            return 9;
        } else if (open.getOfferedCapabilities() != null) {
            return 8;
        } else if (open.getIncomingLocales() != null) {
            return 7;
        } else if (open.getOutgoingLocales() != null) {
            return 6;
        } else if (open.getIdleTimeOut() != null) {
            return 5;
        } else if (open.getChannelMax() != null && !open.getChannelMax().equals(UnsignedShort.MAX_VALUE)) {
            return 4;
        } else if (open.getMaxFrameSize() != null && !open.getMaxFrameSize().equals(UnsignedInteger.MAX_VALUE)) {
            return 3;
        } else if (open.getHostname() != null) {
            return 2;
        } else {
            return 1;
        }
    }
}
