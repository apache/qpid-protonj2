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
package org.apache.qpid.proton4j.codec.encoders.primitives;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.encoders.AbstractPrimitiveTypeEncoder;

/**
 * Encoder of AMQP Binary type values to a byte stream.
 */
public class BinaryTypeEncoder extends AbstractPrimitiveTypeEncoder<Binary> {

    @Override
    public Class<Binary> getTypeClass() {
        return Binary.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Binary value) {
        if (value.getLength() > 255) {
            buffer.writeByte(EncodingCodes.VBIN32);
            buffer.writeInt(value.getLength());
            buffer.writeBytes(value.getArray(), value.getArrayOffset(), value.getLength());
        } else {
            buffer.writeByte(EncodingCodes.VBIN8);
            buffer.writeByte((byte) value.getLength());
            buffer.writeBytes(value.getArray(), value.getArrayOffset(), value.getLength());
        }
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        buffer.writeByte(EncodingCodes.VBIN32);
        for (Object value : values) {
            Binary binary = (Binary) value;
            buffer.writeInt(binary.getLength());
            buffer.writeBytes(binary.getArray(), binary.getArrayOffset(), binary.getLength());
        }
    }
}
