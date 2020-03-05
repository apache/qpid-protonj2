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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Released;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.encoders.AbstractDescribedListTypeEncoder;

/**
 * Encoder of AMQP Released type values to a byte stream
 */
public final class ReleasedTypeEncoder extends AbstractDescribedListTypeEncoder<Released> {

    @Override
    public Class<Released> getTypeClass() {
        return Released.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return Released.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Released.DESCRIPTOR_SYMBOL;
    }

    @Override
    public int getListEncoding(Released value) {
        return EncodingCodes.LIST0;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Released value) {
        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.writeByte(EncodingCodes.SMALLULONG);
        buffer.writeByte(Released.DESCRIPTOR_CODE.byteValue());
        buffer.writeByte(EncodingCodes.LIST0);
    }

    @Override
    public void writeElement(Released source, int index, ProtonBuffer buffer, EncoderState state) {
    }

    @Override
    public int getElementCount(Released value) {
        return 0;
    }
}
