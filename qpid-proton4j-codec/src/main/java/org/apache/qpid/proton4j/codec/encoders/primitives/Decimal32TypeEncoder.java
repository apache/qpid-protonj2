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

import org.apache.qpid.proton4j.amqp.Decimal32;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveTypeEncoder;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Decimal32 type values to a byte stream
 */
public class Decimal32TypeEncoder implements PrimitiveTypeEncoder<Decimal32> {

    @Override
    public Class<Decimal32> getTypeClass() {
        return Decimal32.class;
    }

    @Override
    public void writeType(ByteBuf buffer, EncoderState state, Decimal32 value) {
        buffer.writeByte(EncodingCodes.DECIMAL32);
        buffer.writeInt(value.getBits());
    }

    @Override
    public void writeValue(ByteBuf buffer, EncoderState state, Decimal32 value) {
        buffer.writeInt(value.getBits());
    }
}
