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
package org.apache.qpid.protonj2.codec.encoders.primitives;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractPrimitiveTypeEncoder;
import org.apache.qpid.protonj2.types.UnsignedInteger;

/**
 * Encoder of AMQP UnsignedShort type values to a byte stream.
 */
public final class UnsignedIntegerTypeEncoder extends AbstractPrimitiveTypeEncoder<UnsignedInteger> {

    @Override
    public Class<UnsignedInteger> getTypeClass() {
        return UnsignedInteger.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, UnsignedInteger value) {
        int intValue = value.intValue();

        if (intValue == 0) {
            buffer.writeByte(EncodingCodes.UINT0);
        } else if (intValue > 0 && intValue <= 255) {
            buffer.writeByte(EncodingCodes.SMALLUINT);
            buffer.writeByte((byte) intValue);
        } else {
            buffer.writeByte(EncodingCodes.UINT);
            buffer.writeInt(intValue);
        }
    }

    /**
     * Write the full AMQP type data for the unsigned int to the given byte buffer.
     *
     * This can consist of writing both a type constructor value and the bytes that make up the
     * value of the type being written.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} instance to write the encoding to.
     * @param state
     * 		The {@link EncoderState} for use in encoding operations.
     * @param value
     * 		The unsigned integer single byte value to encode.
     */
    public void writeType(ProtonBuffer buffer, EncoderState state, byte value) {
        if (value == 0) {
            buffer.writeByte(EncodingCodes.UINT0);
        } else {
            buffer.writeByte(EncodingCodes.SMALLUINT);
            buffer.writeByte(value);
        }
    }

    /**
     * Write the full AMQP type data for the unsigned int to the given byte buffer.
     *
     * This can consist of writing both a type constructor value and the bytes that make up the
     * value of the type being written.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} instance to write the encoding to.
     * @param state
     * 		The {@link EncoderState} for use in encoding operations.
     * @param value
     * 		The unsigned int value to encode.
     */
    public void writeType(ProtonBuffer buffer, EncoderState state, int value) {
        if (value == 0) {
            buffer.writeByte(EncodingCodes.UINT0);
        } else if (value > 0 && value <= 255) {
            buffer.writeByte(EncodingCodes.SMALLUINT);
            buffer.writeByte((byte) value);
        } else {
            buffer.writeByte(EncodingCodes.UINT);
            buffer.writeInt(value);
        }
    }

    /**
     * Write the full AMQP type data for the unsigned integer to the given byte buffer.
     *
     * This can consist of writing both a type constructor value and the bytes that make up the
     * value of the type being written.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} instance to write the encoding to.
     * @param state
     * 		The {@link EncoderState} for use in encoding operations.
     * @param value
     * 		The long value to encode as an unsigned integer.
     */
    public void writeType(ProtonBuffer buffer, EncoderState state, long value) {
        if (value < 0L || value >= (1L << 32)) {
            throw new IllegalArgumentException("Value \"" + value + "\" lies outside the range [" + 0L + "-" + (1L << 32) + ").");
        }

        int intValue = (int) value;

        if (intValue == 0) {
            buffer.writeByte(EncodingCodes.UINT0);
        } else if (intValue > 0 && intValue <= 255) {
            buffer.writeByte(EncodingCodes.SMALLUINT);
            buffer.writeByte((byte) intValue);
        } else {
            buffer.writeByte(EncodingCodes.UINT);
            buffer.writeInt(intValue);
        }
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        buffer.writeByte(EncodingCodes.UINT);
        for (Object value : values) {
            buffer.writeInt(((UnsignedInteger) value).intValue());
        }
    }
}
