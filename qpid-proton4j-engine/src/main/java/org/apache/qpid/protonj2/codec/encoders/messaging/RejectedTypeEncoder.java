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
import org.apache.qpid.protonj2.types.messaging.Rejected;

/**
 * Encoder of AMQP Rejected type values to a byte stream.
 */
public final class RejectedTypeEncoder extends AbstractDescribedListTypeEncoder<Rejected> {

    @Override
    public UnsignedLong getDescriptorCode() {
        return Rejected.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Rejected.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Rejected> getTypeClass() {
        return Rejected.class;
    }

    @Override
    public void writeElement(Rejected source, int index, ProtonBuffer buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeObject(buffer, state, source.getError());
                break;
            default:
                throw new IllegalArgumentException("Unknown Rejected value index: " + index);
        }
    }

    @Override
    public int getListEncoding(Rejected value) {
        if (value.getError() != null) {
            return EncodingCodes.LIST32;
        } else {
            return EncodingCodes.LIST8;
        }
    }

    @Override
    public int getElementCount(Rejected value) {
        if (value.getError() != null) {
            return 1;
        } else {
            return 0;
        }
    }
}
