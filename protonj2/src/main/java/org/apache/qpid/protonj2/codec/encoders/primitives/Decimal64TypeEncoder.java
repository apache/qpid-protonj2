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
import org.apache.qpid.protonj2.types.Decimal64;

/**
 * Encoder of AMQP Decimal64 type values to a byte stream
 */
public final class Decimal64TypeEncoder extends AbstractPrimitiveTypeEncoder<Decimal64> {

    @Override
    public Class<Decimal64> getTypeClass() {
        return Decimal64.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Decimal64 value) {
        buffer.writeByte(EncodingCodes.DECIMAL64);
        buffer.writeLong(value.getBits());
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        buffer.writeByte(EncodingCodes.DECIMAL64);
        for (Object value : values) {
            buffer.writeLong(((Decimal64) value).getBits());
        }
    }
}
