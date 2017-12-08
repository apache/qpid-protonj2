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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveTypeEncoder;

/**
 * Encoder of AMQP Integer type values to a byte stream.
 */
public class IntegerTypeEncoder implements PrimitiveTypeEncoder<Integer> {

    @Override
    public Class<Integer> getTypeClass() {
        return Integer.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Integer value) {
        writeType(buffer, state, value.intValue());
    }

    public void writeType(ProtonBuffer buffer, EncoderState state, int value) {
        if (value >= -128 && value <= 127) {
            buffer.writeByte(EncodingCodes.SMALLINT);
            buffer.writeByte((byte) value);
        } else {
            buffer.writeByte(EncodingCodes.INT);
            buffer.writeInt(value);
        }
    }

    @Override
    public void writeArrayElements(ProtonBuffer buffer, EncoderState state, Integer[] values) {
        buffer.writeByte(EncodingCodes.INT);
        for (Integer value : values) {
            buffer.writeInt(value.intValue());
        }
    }

    public void writeArrayElements(ProtonBuffer buffer, EncoderState state, int[] values) {
        buffer.writeByte(EncodingCodes.INT);
        for (int value : values) {
            buffer.writeInt(value);
        }
    }
}
