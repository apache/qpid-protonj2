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
package org.apache.qpid.protonj2.codec.encoders.messaging;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Data;

/**
 * Encoder of AMQP Data type values to a byte stream.
 */
public final class DataTypeEncoder extends AbstractDescribedTypeEncoder<Data> {

    private static final byte[] DATA_PREAMBLE = new byte[] {
        EncodingCodes.DESCRIBED_TYPE_INDICATOR, EncodingCodes.SMALLULONG, Data.DESCRIPTOR_CODE.byteValue()
    };

    @Override
    public Class<Data> getTypeClass() {
        return Data.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Data.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Data.DESCRIPTOR_SYMBOL;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Data value) {
        buffer.writeBytes(DATA_PREAMBLE);

        final int dataLength = value.getDataLength();

        if (dataLength > 255) {
            buffer.ensureWritable(dataLength + Long.BYTES);
            buffer.writeByte(EncodingCodes.VBIN32);
            buffer.writeInt(dataLength);
        } else {
            buffer.ensureWritable(dataLength + Short.BYTES);
            buffer.writeByte(EncodingCodes.VBIN8);
            buffer.writeByte((byte) dataLength);
        }

        value.copyTo(buffer);
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        // Write the Array Type encoding code, we don't optimize here.
        buffer.writeByte(EncodingCodes.ARRAY32);

        final int startIndex = buffer.getWriteOffset();

        // Reserve space for the size and write the count of list elements.
        buffer.writeInt(0);
        buffer.writeInt(values.length);

        writeRawArray(buffer, state, values);

        // Move back and write the size
        final int endIndex = buffer.getWriteOffset();
        final long writeSize = endIndex - startIndex - Integer.BYTES;

        if (writeSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given array, encoded size to large: " + writeSize);
        }

        buffer.setInt(startIndex, (int) writeSize);
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        state.getEncoder().writeUnsignedLong(buffer, state, getDescriptorCode());

        buffer.writeByte(EncodingCodes.VBIN32);
        for (Object value : values) {
            final ProtonBuffer binary = ((Data) value).getBuffer();
            buffer.writeInt(binary.getReadableBytes());
            buffer.writeBytes(binary);
        }
    }
}
