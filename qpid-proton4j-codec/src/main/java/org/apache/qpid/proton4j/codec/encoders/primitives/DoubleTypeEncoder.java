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
 * Encoder of AMQP Double type values to a byte stream.
 */
public class DoubleTypeEncoder implements PrimitiveTypeEncoder<Double> {

    @Override
    public Class<Double> getTypeClass() {
        return Double.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Double value) {
        buffer.writeByte(EncodingCodes.DOUBLE);
        buffer.writeDouble(value.doubleValue());
    }

    public void writeType(ProtonBuffer buffer, EncoderState state, double value) {
        buffer.writeByte(EncodingCodes.DOUBLE);
        buffer.writeDouble(value);
    }

    @Override
    public void writeValue(ProtonBuffer buffer, EncoderState state, Double value) {
        buffer.writeDouble(value.doubleValue());
    }

    public void writeValue(ProtonBuffer buffer, EncoderState state, double value) {
        buffer.writeDouble(value);
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Double[] values) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Long.BYTES * values.length) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given double array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(values.length);
        buffer.writeByte(EncodingCodes.DOUBLE);
        for (Double value : values) {
            buffer.writeDouble(value.doubleValue());
        }
    }

    public void writeArray(ProtonBuffer buffer, EncoderState state, double[] values) {
        buffer.writeByte(EncodingCodes.ARRAY32);

        // Array Size -> Total Bytes + Number of elements + Type Code
        long size = (Long.BYTES * values.length) + Integer.BYTES + Byte.BYTES;

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given double array, encoded size to large: " + size);
        }

        buffer.writeInt((int) size);
        buffer.writeInt(values.length);
        buffer.writeByte(EncodingCodes.DOUBLE);
        for (double value : values) {
            buffer.writeDouble(value);
        }
    }
}
