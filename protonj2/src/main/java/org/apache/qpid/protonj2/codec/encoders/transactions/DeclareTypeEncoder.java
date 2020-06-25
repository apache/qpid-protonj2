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
package org.apache.qpid.protonj2.codec.encoders.transactions;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.transactions.Declare;

/**
 * Encoder of AMQP Declare type values to a byte stream.
 */
public final class DeclareTypeEncoder extends AbstractDescribedListTypeEncoder<Declare> {

    @Override
    public UnsignedLong getDescriptorCode() {
        return Declare.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return Declare.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<Declare> getTypeClass() {
        return Declare.class;
    }

    @Override
    public void writeElement(Declare declare, int index, ProtonBuffer buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeObject(buffer, state, declare.getGlobalId());
                break;
            default:
                throw new IllegalArgumentException("Unknown Declare value index: " + index);
        }
    }

    @Override
    public int getListEncoding(Declare value) {
        if (value.getGlobalId() != null) {
            return EncodingCodes.LIST32;
        } else {
            return EncodingCodes.LIST0;
        }
    }

    @Override
    public int getElementCount(Declare declare) {
        if (declare.getGlobalId() != null) {
            return 1;
        } else {
            return 0;
        }
    }
}
