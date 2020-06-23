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
package org.apache.qpid.proton4j.codec.encoders.messaging;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.encoders.AbstractDescribedTypeEncoder;
import org.apache.qpid.proton4j.types.Binary;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedLong;
import org.apache.qpid.proton4j.types.messaging.Data;

/**
 * Encoder of AMQP Data type values to a byte stream.
 */
public final class DataTypeEncoder extends AbstractDescribedTypeEncoder<Data> {

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
        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Data.DESCRIPTOR_CODE.byteValue());

        state.getEncoder().writeBinary(buffer, state, value.getValue());
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        // Write the Array Type encoding code, we don't optimize here.
        buffer.writeByte(EncodingCodes.ARRAY32);

        final int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        buffer.writeInt(0);
        buffer.writeInt(values.length);

        writeRawArray(buffer, state, values);

        // Move back and write the size
        final int endIndex = buffer.getWriteIndex();
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
            final Binary binary = ((Data) value).getValue();
            buffer.writeInt(binary.getLength());
            buffer.writeBytes(binary.getArray(), binary.getArrayOffset(), binary.getLength());
        }
    }
}
