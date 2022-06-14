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
import org.apache.qpid.protonj2.codec.encoders.AbstractDescribedMapTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;

/**
 * Encoder of AMQP MessageAnnotations type values to a byte stream.
 */
public final class MessageAnnotationsTypeEncoder extends AbstractDescribedMapTypeEncoder<Symbol, Object, MessageAnnotations> {

    @Override
    public Class<MessageAnnotations> getTypeClass() {
        return MessageAnnotations.class;
    }

    @Override
    public UnsignedLong getDescriptorCode() {
        return MessageAnnotations.DESCRIPTOR_CODE;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return MessageAnnotations.DESCRIPTOR_SYMBOL;
    }

    @Override
    public boolean hasMap(MessageAnnotations value) {
        return value.getValue() != null;
    }

    @Override
    public int getMapSize(MessageAnnotations value) {
        if (value.getValue() != null) {
            return value.getValue().size();
        } else {
            return 0;
        }
    }

    @Override
    public void writeMapEntries(ProtonBuffer buffer, EncoderState state, MessageAnnotations annotations) {
        // Write the Map elements and then compute total size written.
        annotations.getValue().forEach((key, value) -> {
            state.getEncoder().writeSymbol(buffer, state, key);
            state.getEncoder().writeObject(buffer, state, value);
        });
    }
}
