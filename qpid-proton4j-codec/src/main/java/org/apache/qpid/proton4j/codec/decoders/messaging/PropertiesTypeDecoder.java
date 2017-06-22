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
package org.apache.qpid.proton4j.codec.decoders.messaging;

import java.io.IOException;
import java.util.Date;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.messaging.Properties;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.DescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ListTypeDecoder;

import io.netty.buffer.ByteBuf;

/**
 * Decoder of AMQP Properties type values from a byte stream
 */
public class PropertiesTypeDecoder implements DescribedTypeDecoder<Properties>, ListTypeDecoder.ListEntryHandler {

    private static final UnsignedLong descriptorCode = UnsignedLong.valueOf(0x0000000000000073L);
    private static final Symbol descriptorSymbol = Symbol.valueOf("amqp:properties:list");

    @Override
    public UnsignedLong getDescriptorCode() {
        return descriptorCode;
    }

    @Override
    public Symbol getDescriptorSymbol() {
        return descriptorSymbol;
    }

    @Override
    public Class<Properties> getTypeClass() {
        return Properties.class;
    }

    @Override
    public Properties readValue(ByteBuf buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (!(decoder instanceof ListTypeDecoder)) {
            throw new IOException("Expected List type indicator but got decoder for type: " + decoder.getTypeClass().getName());
        }

        ListTypeDecoder listDecoder = (ListTypeDecoder) decoder;
        Properties properties = new Properties();

        listDecoder.readValue(buffer, state, this, properties);

        return properties;
    }

    @Override
    public void onListEntry(int index, Object entry, Object target) {
        Properties properties = (Properties) target;

        switch (index) {
            case 0:
                properties.setMessageId(entry);
                break;
            case 1:
                properties.setUserId((Binary) entry);
                break;
            case 2:
                properties.setTo((String) entry);
                break;
            case 3:
                properties.setSubject((String) entry);
                break;
            case 4:
                properties.setReplyTo((String) entry);
                break;
            case 5:
                properties.setCorrelationId(entry);
                break;
            case 6:
                properties.setContentType((Symbol) entry);
                break;
            case 7:
                properties.setContentEncoding((Symbol) entry);
                break;
            case 8:
                properties.setAbsoluteExpiryTime(entry == null ? null : new Date((Long) entry));
                break;
            case 9:
                properties.setCreationTime(entry == null ? null : new Date((Long) entry));
                break;
            case 10:
                properties.setGroupId((String) entry);
                break;
            case 11:
                properties.setGroupSequence((UnsignedInteger) entry);
                break;
            case 12:
                properties.setReplyToGroupId((String) entry);
                break;
            default:
                throw new IllegalStateException("To many entries in Properties encoding");
        }
    }
}
