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
package org.apache.qpid.proton4j.codec.encoders.transactions;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedLong;
import org.apache.qpid.proton4j.types.transactions.TransactionalState;

/**
 * Encoder of AMQP TransactionState type values to a byte stream.
 */
public final class TransactionStateTypeEncoder extends AbstractDescribedListTypeEncoder<TransactionalState> {

    @Override
    public UnsignedLong getDescriptorCode() {
        return TransactionalState.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return TransactionalState.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<TransactionalState> getTypeClass() {
        return TransactionalState.class;
    }

    @Override
    public void writeElement(TransactionalState txState, int index, ProtonBuffer buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeBinary(buffer, state, txState.getTxnId());
                break;
            case 1:
                state.getEncoder().writeObject(buffer, state, txState.getOutcome());
                break;
            default:
                throw new IllegalArgumentException("Unknown TransactionalState value index: " + index);
        }
    }

    @Override
    public int getListEncoding(TransactionalState value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getElementCount(TransactionalState txState) {
        if (txState.getOutcome() != null) {
            return 2;
        } else {
            return 1;
        }
    }
}
