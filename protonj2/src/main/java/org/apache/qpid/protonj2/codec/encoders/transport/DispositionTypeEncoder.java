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
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.transport.Disposition;

/**
 * Encoder of AMQP Disposition type values to a byte stream
 */
public final class DispositionTypeEncoder extends AbstractDescribedListTypeEncoder<Disposition> {

    private static final byte[] ACCEPTED_ENCODING = new byte[] {
        EncodingCodes.DESCRIBED_TYPE_INDICATOR,
        EncodingCodes.SMALLULONG,
        Accepted.DESCRIPTOR_CODE.byteValue(),
        EncodingCodes.LIST0
    };

    @Override
    public UnsignedLong getDescriptorCode() {
        return Disposition.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Disposition.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Disposition> getTypeClass() {
        return Disposition.class;
    }

    /*
     * This assumes that the value was already check be the setter in Disposition
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
    public void writeElement(Disposition disposition, int index, ProtonBuffer buffer, Encoder encoder, EncoderState state) {
        if (disposition.hasElement(index)) {
            switch (index) {
                case 0:
                    buffer.writeByte(disposition.getRole().encodingCode());
                    break;
                case 1:
                    writeCheckedUnsignedInteger(disposition.getFirst(), buffer);
                    break;
                case 2:
                    writeCheckedUnsignedInteger(disposition.getLast(), buffer);
                    break;
                case 3:
                    buffer.writeByte(disposition.getSettled() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                    break;
                case 4:
                    if (disposition.getState() == Accepted.getInstance()) {
                        buffer.writeBytes(ACCEPTED_ENCODING);
                    } else {
                        encoder.writeObject(buffer, state, disposition.getState());
                    }
                    break;
                case 5:
                    buffer.writeByte(disposition.getBatchable() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown Disposition value index: " + index);
            }
        } else {
            buffer.writeByte(EncodingCodes.NULL);
        }
    }

    @Override
    public byte getListEncoding(Disposition value) {
        if (value.getState() == null) {
            return EncodingCodes.LIST8;
        } else if (value.getState() == Accepted.getInstance() || value.getState() == Released.getInstance()) {
            return EncodingCodes.LIST8;
        } else {
            return EncodingCodes.LIST32;
        }
    }

    @Override
    public int getElementCount(Disposition disposition) {
        return disposition.getElementCount();
    }

    @Override
    public int getMinElementCount() {
        return 2;
    }
}
