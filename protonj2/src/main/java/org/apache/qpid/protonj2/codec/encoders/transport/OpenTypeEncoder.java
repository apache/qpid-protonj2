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
import org.apache.qpid.protonj2.types.transport.Open;

/**
 * Encoder of AMQP Open type values to a byte stream.
 */
public final class OpenTypeEncoder extends AbstractDescribedListTypeEncoder<Open> {

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
                if (open.hasContainerId()) {
                    state.getEncoder().writeString(buffer, state, open.getContainerId());
                } else {
                    state.getEncoder().writeNull(buffer, state);
                }
                break;
            case 1:
                if (open.hasHostname()) {
                    state.getEncoder().writeString(buffer, state, open.getHostname());
                } else {
                    state.getEncoder().writeNull(buffer, state);
                }
                break;
            case 2:
                if (open.hasMaxFrameSize()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, open.getMaxFrameSize());
                } else {
                    state.getEncoder().writeNull(buffer, state);
                }
                break;
            case 3:
                if (open.hasChannelMax()) {
                    state.getEncoder().writeUnsignedShort(buffer, state, open.getChannelMax());
                } else {
                    state.getEncoder().writeNull(buffer, state);
                }
                break;
            case 4:
                if (open.hasIdleTimeout()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, open.getIdleTimeout());
                } else {
                    state.getEncoder().writeNull(buffer, state);
                }
                break;
            case 5:
                if (open.hasOutgoingLocales()) {
                    state.getEncoder().writeArray(buffer, state, open.getOutgoingLocales());
                } else {
                    state.getEncoder().writeNull(buffer, state);
                }
                break;
            case 6:
                if (open.hasIncomingLocales()) {
                    state.getEncoder().writeArray(buffer, state, open.getIncomingLocales());
                } else {
                    state.getEncoder().writeNull(buffer, state);
                }
                break;
            case 7:
                if (open.hasOfferedCapabilities()) {
                    state.getEncoder().writeArray(buffer, state, open.getOfferedCapabilities());
                } else {
                    state.getEncoder().writeNull(buffer, state);
                }
                break;
            case 8:
                if (open.hasDesiredCapabilities()) {
                    state.getEncoder().writeArray(buffer, state, open.getDesiredCapabilities());
                } else {
                    state.getEncoder().writeNull(buffer, state);
                }
                break;
            case 9:
                if (open.hasProperties()) {
                    state.getEncoder().writeMap(buffer, state, open.getProperties());
                } else {
                    state.getEncoder().writeNull(buffer, state);
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown Open value index: " + index);
        }
    }

    @Override
    public byte getListEncoding(Open value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getElementCount(Open open) {
        return open.getElementCount();
    }

    @Override
    public int getMinElementCount() {
        return 1;
    }
}
