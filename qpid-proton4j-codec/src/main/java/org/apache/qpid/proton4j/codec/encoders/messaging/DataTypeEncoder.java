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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Data;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DescribedTypeEncoder;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeEncoder;

/**
 * Encoder of AMQP Data type values to a byte stream.
 */
public class DataTypeEncoder implements DescribedTypeEncoder<Data> {

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
        state.getEncoder().writeUnsignedLong(buffer, state, getDescriptorCode());
        state.getEncoder().writeBinary(buffer, state, value.getValue());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void writeArrayElements(ProtonBuffer buffer, EncoderState state, Data[] value) {
        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        state.getEncoder().writeUnsignedLong(buffer, state, getDescriptorCode());

        Binary[] binaryArray = new Binary[value.length];
        for (int i = 0; i < value.length; ++i) {
            binaryArray[i] = value[i].getValue();
        }

        TypeEncoder encoder = state.getEncoder().getTypeEncoder(Binary.class);
        if (encoder == null) {
            throw new IllegalStateException("No TypeEncoder found for Binary elements");
        }

        encoder.writeArrayElements(buffer, state, binaryArray);
    }
}
