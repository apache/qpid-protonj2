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
package org.apache.qpid.proton4j.codec.encoders.transport;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.encoders.AbstractDescribedListTypeEncoder;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedLong;
import org.apache.qpid.proton4j.types.transport.ErrorCondition;

/**
 * Encoder of AMQP ErrorCondition type values to a byte stream
 */
public final class ErrorConditionTypeEncoder extends AbstractDescribedListTypeEncoder<ErrorCondition> {

    @Override
    public UnsignedLong getDescriptorCode() {
        return ErrorCondition.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return ErrorCondition.DESCRIPTOR_SYMBOL;
    }

    @Override
    public Class<ErrorCondition> getTypeClass() {
        return ErrorCondition.class;
    }

    @Override
    public void writeElement(ErrorCondition error, int index, ProtonBuffer buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeSymbol(buffer, state, error.getCondition());
                break;
            case 1:
                state.getEncoder().writeString(buffer, state, error.getDescription());
                break;
            case 2:
                state.getEncoder().writeMap(buffer, state, error.getInfo());
                break;
            default:
                throw new IllegalArgumentException("Unknown ErrorCondition value index: " + index);
        }
    }

    @Override
    public int getListEncoding(ErrorCondition value) {
        return EncodingCodes.LIST32;
    }

    @Override
    public int getElementCount(ErrorCondition error) {
        if (error.getInfo() != null) {
            return 3;
        } else if (error.getDescription() != null) {
            return 2;
        } else {
            return 1;
        }
    }
}
