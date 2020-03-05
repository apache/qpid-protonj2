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
package org.apache.qpid.proton4j.codec.encoders.security;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.security.SaslInit;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.encoders.AbstractDescribedListTypeEncoder;

/**
 * Encoder of AMQP SaslInit type values to a byte stream
 */
public final class SaslInitTypeEncoder extends AbstractDescribedListTypeEncoder<SaslInit> {

    @Override
    public Class<SaslInit> getTypeClass() {
        return SaslInit.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return SaslInit.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return SaslInit.DESCRIPTOR_SYMBOL;
    }

    @Override
    public void writeElement(SaslInit init, int index, ProtonBuffer buffer, EncoderState state) {
        switch (index) {
            case 0:
                state.getEncoder().writeSymbol(buffer, state, init.getMechanism());
                break;
            case 1:
                state.getEncoder().writeBinary(buffer, state, init.getInitialResponse());
                break;
            case 2:
                state.getEncoder().writeString(buffer, state, init.getHostname());
                break;
            default:
                throw new IllegalArgumentException("Unknown SaslInit value index: " + index);
        }
    }

    @Override
    public int getElementCount(SaslInit init) {
        if (init.getHostname() != null) {
            return 3;
        } else if (init.getInitialResponse() != null) {
            return 2;
        } else {
            return 1;
        }
    }
}
