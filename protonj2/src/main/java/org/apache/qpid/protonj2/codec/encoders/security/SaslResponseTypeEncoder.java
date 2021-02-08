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
package org.apache.qpid.protonj2.codec.encoders.security;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.security.SaslResponse;

/**
 * Encoder of AMQP SaslResponse type values to a byte stream
 */
public final class SaslResponseTypeEncoder extends AbstractDescribedListTypeEncoder<SaslResponse> {

    @Override
    public Class<SaslResponse> getTypeClass() {
        return SaslResponse.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return SaslResponse.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return SaslResponse.DESCRIPTOR_SYMBOL;
    }

    @Override
    public byte getListEncoding(SaslResponse value) {
        if (value.getResponse().getReadableBytes() < 255) {
            return EncodingCodes.LIST8;
        } else {
            return EncodingCodes.LIST32;
        }
    }

    @Override
    public void writeElement(SaslResponse response, int index, ProtonBuffer buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeBinary(buffer, state, response.getResponse());
                break;
            default:
                throw new IllegalArgumentException("Unknown SaslResponse value index: " + index);
        }
    }

    @Override
    public int getElementCount(SaslResponse challenge) {
        return 1;
    }

    @Override
    public int getMinElementCount() {
        return 1;
    }
}
