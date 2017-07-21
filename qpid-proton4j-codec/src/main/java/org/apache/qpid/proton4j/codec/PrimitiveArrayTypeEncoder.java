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
package org.apache.qpid.proton4j.codec;

import java.util.UUID;

import org.apache.qpid.proton4j.amqp.Decimal128;
import org.apache.qpid.proton4j.amqp.Decimal32;
import org.apache.qpid.proton4j.amqp.Decimal64;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedByte;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.UnsignedShort;

import io.netty.buffer.ByteBuf;

public interface PrimitiveArrayTypeEncoder extends PrimitiveTypeEncoder<Object> {

    @Override
    default boolean isArryTypeDecoder() {
        return true;
    }

    @Override
    default Class<Object> getTypeClass() {
        return Object.class;
    }

    void writeArray(ByteBuf buffer, EncoderState state, Object[] value);

    void writeArray(ByteBuf buffer, EncoderState state, boolean[] value);

    void writeArray(ByteBuf buffer, EncoderState state, byte[] value);

    void writeArray(ByteBuf buffer, EncoderState state, short[] value);

    void writeArray(ByteBuf buffer, EncoderState state, int[] value);

    void writeArray(ByteBuf buffer, EncoderState state, long[] value);

    void writeArray(ByteBuf buffer, EncoderState state, float[] value);

    void writeArray(ByteBuf buffer, EncoderState state, double[] value);

    void writeArray(ByteBuf buffer, EncoderState state, char[] value);

    void writeArray(ByteBuf buffer, EncoderState state, Decimal32[] value);

    void writeArray(ByteBuf buffer, EncoderState state, Decimal64[] value);

    void writeArray(ByteBuf buffer, EncoderState state, Decimal128[] value);

    void writeArray(ByteBuf buffer, EncoderState state, Symbol[] value);

    void writeArray(ByteBuf buffer, EncoderState state, UnsignedByte[] value);

    void writeArray(ByteBuf buffer, EncoderState state, UnsignedShort[] value);

    void writeArray(ByteBuf buffer, EncoderState state, UnsignedInteger[] value);

    void writeArray(ByteBuf buffer, EncoderState state, UnsignedLong[] value);

    void writeArray(ByteBuf buffer, EncoderState state, UUID[] value);

}
