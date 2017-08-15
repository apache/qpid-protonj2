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
package org.apache.qpid.proton4j.codec.encoders;

import org.apache.qpid.proton4j.amqp.DescribedType;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeEncoder;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Described Types to a byte stream.
 */
public class UnknownDescribedTypeEncoder implements TypeEncoder<DescribedType> {

    @Override
    public Class<DescribedType> getTypeClass() {
        return DescribedType.class;
    }

    @Override
    public void writeType(ByteBuf buffer, EncoderState state, DescribedType value) {
        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        state.getEncoder().writeObject(buffer, state, value.getDescriptor());
        state.getEncoder().writeObject(buffer, state, value.getDescribed());
    }

    @Override
    public void writeValue(ByteBuf buffer, EncoderState state, DescribedType value) {
        state.getEncoder().writeObject(buffer, state, value.getDescribed());
    }
}
