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
package org.apache.qpid.protonj2.codec.encoders.messaging;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.Modified;

/**
 * Encoder of AMQP Modified type values to a byte stream.
 */
public final class ModifiedTypeEncoder extends AbstractDescribedListTypeEncoder<Modified> {

    @Override
    public UnsignedLong getDescriptorCode() {
        return Modified.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Modified.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Modified> getTypeClass() {
        return Modified.class;
    }

    @Override
    public void writeElement(Modified source, int index, ProtonBuffer buffer, EncoderState state) {
        switch (index) {
            case 0:
                buffer.writeByte(source.isDeliveryFailed() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                break;
            case 1:
                buffer.writeByte(source.isUndeliverableHere() ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
                break;
            case 2:
                state.getEncoder().writeMap(buffer, state, source.getMessageAnnotations());
                break;
            default:
                throw new IllegalArgumentException("Unknown Modified value index: " + index);
        }
    }

    @Override
    public byte getListEncoding(Modified value) {
        if (value.getMessageAnnotations() != null) {
            return EncodingCodes.LIST32;
        } else {
            return EncodingCodes.LIST8;
        }
    }

    @Override
    public int getElementCount(Modified value) {
        if (value.getMessageAnnotations() != null) {
            return 3;
        } else if (value.isUndeliverableHere()) {
            return 2;
        } else if (value.isDeliveryFailed()) {
            return 1;
        } else {
            return 0;
        }
    }
}
