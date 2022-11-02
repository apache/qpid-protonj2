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
    public void writeElement(Begin begin, int index, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        if (begin.hasElement(index)) {
            switch (index) {
                case 0:
                    encoder.writeUnsignedShort(buffer, state, begin.getRemoteChannel());
                    break;
                case 1:
                    encoder.writeUnsignedInteger(buffer, state, begin.getNextOutgoingId());
                    break;
                case 2:
                    encoder.writeUnsignedInteger(buffer, state, begin.getIncomingWindow());
                    break;
                case 3:
                    encoder.writeUnsignedInteger(buffer, state, begin.getOutgoingWindow());
                    break;
                case 4:
                    encoder.writeUnsignedInteger(buffer, state, begin.getHandleMax());
                    break;
                case 5:
                    encoder.writeArray(buffer, state, begin.getOfferedCapabilities());
                    break;
                case 6:
                    encoder.writeArray(buffer, state, begin.getDesiredCapabilities());
                    break;
                case 7:
                    encoder.writeMap(buffer, state, begin.getProperties());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown Begin value index: " + index);
            }
        } else {
            buffer.writeByte(EncodingCodes.NULL);
        }
    }

    @Override
    public byte getListEncoding(Begin value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getElementCount(Begin begin) {
        return begin.getElementCount();
    }

    @Override
    public int getMinElementCount() {
        return 4;
    }
}
