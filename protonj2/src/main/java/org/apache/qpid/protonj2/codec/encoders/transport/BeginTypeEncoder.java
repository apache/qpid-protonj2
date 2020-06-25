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
import org.apache.qpid.protonj2.types.transport.Begin;

/**
 * Encoder of AMQP Begin type values to a byte stream.
 */
public final class BeginTypeEncoder extends AbstractDescribedListTypeEncoder<Begin> {

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
                if (begin.hasRemoteChannel()) {
                    state.getEncoder().writeUnsignedShort(buffer, state, begin.getRemoteChannel());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 1:
                if (begin.hasNextOutgoingId()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, begin.getNextOutgoingId());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 2:
                if (begin.hasIncomingWindow()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, begin.getIncomingWindow());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 3:
                if (begin.hasOutgoingWindow()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, begin.getOutgoingWindow());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 4:
                if (begin.hasHandleMax()) {
                    state.getEncoder().writeUnsignedInteger(buffer, state, begin.getHandleMax());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 5:
                if (begin.hasOfferedCapabilites()) {
                    state.getEncoder().writeArray(buffer, state, begin.getOfferedCapabilities());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 6:
                if (begin.hasDesiredCapabilites()) {
                    state.getEncoder().writeArray(buffer, state, begin.getDesiredCapabilities());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
                break;
            case 7:
                if (begin.hasProperties()) {
                    state.getEncoder().writeMap(buffer, state, begin.getProperties());
                } else {
                    buffer.writeByte(EncodingCodes.NULL);
                }
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
        return begin.getElementCount();
    }
}
