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

package org.apache.qpid.protonj2.codec;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.types.messaging.AmqpSequence;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Section;

/**
 * AMQP Section type specific encoder that uses any {@link Encoder} to
 * cache the specific type encoders for various section types and use them
 * directly instead of looking them up via calls to {@link Encoder#writeObject(org.apache.qpid.protonj2.buffer.ProtonBuffer, EncoderState, Object)}
 */
public final class SectionEncoder {

    private final TypeEncoder<ApplicationProperties> apEncoder;
    private final TypeEncoder<MessageAnnotations> maEncoder;
    private final TypeEncoder<DeliveryAnnotations> daEncoder;
    private final TypeEncoder<Properties> propertiesEncoder;
    private final TypeEncoder<Header> headerEncoder;
    private final TypeEncoder<Footer> footerEncoder;

    private final TypeEncoder<Data> dataEncoder;
    private final TypeEncoder<AmqpSequence<?>> sequenceEncoder;
    private final TypeEncoder<AmqpValue<?>> valueEncoder;

    private final EncoderState encoderState;

    @SuppressWarnings("unchecked")
    public SectionEncoder(Encoder encoder) {
        encoderState = encoder.newEncoderState();

        apEncoder = (TypeEncoder<ApplicationProperties>) encoder.getTypeEncoder(ApplicationProperties.class);
        maEncoder = (TypeEncoder<MessageAnnotations>) encoder.getTypeEncoder(MessageAnnotations.class);
        daEncoder = (TypeEncoder<DeliveryAnnotations>) encoder.getTypeEncoder(DeliveryAnnotations.class);
        propertiesEncoder = (TypeEncoder<Properties>) encoder.getTypeEncoder(Properties.class);
        headerEncoder = (TypeEncoder<Header>) encoder.getTypeEncoder(Header.class);
        footerEncoder = (TypeEncoder<Footer>) encoder.getTypeEncoder(Footer.class);

        dataEncoder = (TypeEncoder<Data>) encoder.getTypeEncoder(Data.class);
        sequenceEncoder = (TypeEncoder<AmqpSequence<?>>) encoder.getTypeEncoder(AmqpSequence.class);
        valueEncoder = (TypeEncoder<AmqpValue<?>>) encoder.getTypeEncoder(AmqpValue.class);
    }

    /**
     * Writes the given section using the cached encoder for that section types
     *
     * @param buffer
     * 		The buffer to write the encoding to
     * @param section
     *      The section to write using one of the cached encoders
     */
    public void write(ProtonBuffer buffer, Section<?> section) {
        try {
            switch (section.getType()) {
            case AmqpSequence:
                sequenceEncoder.writeType(buffer, encoderState, (AmqpSequence<?>) section);
                break;
            case AmqpValue:
                valueEncoder.writeType(buffer, encoderState, (AmqpValue<?>) section);
                break;
            case ApplicationProperties:
                apEncoder.writeType(buffer, encoderState, (ApplicationProperties) section);
                break;
            case Data:
                dataEncoder.writeType(buffer, encoderState, (Data) section);
                break;
            case DeliveryAnnotations:
                daEncoder.writeType(buffer, encoderState, (DeliveryAnnotations) section);
                break;
            case Footer:
                footerEncoder.writeType(buffer, encoderState, (Footer) section);
                break;
            case Header:
                headerEncoder.writeType(buffer, encoderState, (Header) section);
                break;
            case MessageAnnotations:
                maEncoder.writeType(buffer, encoderState, (MessageAnnotations) section);
                break;
            case Properties:
                propertiesEncoder.writeType(buffer, encoderState, (Properties) section);
                break;
            default:
                break;
            }
        } finally {
            encoderState.reset();
        }
    }

    /**
     * Writes the given section using the cached encoder for that section types
     *
     * @param buffer
     * 		The buffer to write the encoding to
     * @param properties
     *      The section to write using the cached encoder.
     */
    public void write(ProtonBuffer buffer, ApplicationProperties properties) {
        try {
            apEncoder.writeType(buffer, encoderState, properties);
        } finally {
            encoderState.reset();
        }
    }

    /**
     * Writes the given section using the cached encoder for that section types
     *
     * @param buffer
     * 		The buffer to write the encoding to
     * @param annotations
     *      The section to write using the cached encoder.
     */
    public void write(ProtonBuffer buffer, MessageAnnotations annotations) {
        try {
            maEncoder.writeType(buffer, encoderState, annotations);
        } finally {
            encoderState.reset();
        }
    }

    /**
     * Writes the given section using the cached encoder for that section types
     *
     * @param buffer
     * 		The buffer to write the encoding to
     * @param annotations
     *      The section to write using the cached encoder.
     */
    public void write(ProtonBuffer buffer, DeliveryAnnotations annotations) {
        try {
            daEncoder.writeType(buffer, encoderState, annotations);
        } finally {
            encoderState.reset();
        }
    }

    /**
     * Writes the given section using the cached encoder for that section types
     *
     * @param buffer
     * 		The buffer to write the encoding to
     * @param properties
     *      The section to write using the cached encoder.
     */
    public void write(ProtonBuffer buffer, Properties properties) {
        try {
            propertiesEncoder.writeType(buffer, encoderState, properties);
        } finally {
            encoderState.reset();
        }
    }

    /**
     * Writes the given section using the cached encoder for that section types
     *
     * @param buffer
     * 		The buffer to write the encoding to
     * @param header
     *      The section to write using the cached encoder.
     */
    public void write(ProtonBuffer buffer, Header header) {
        try {
            headerEncoder.writeType(buffer, encoderState, header);
        } finally {
            encoderState.reset();
        }
    }

    /**
     * Writes the given section using the cached encoder for that section types
     *
     * @param buffer
     * 		The buffer to write the encoding to
     * @param footer
     *      The section to write using the cached encoder.
     */
    public void write(ProtonBuffer buffer, Footer footer) {
        try {
            footerEncoder.writeType(buffer, encoderState, footer);
        } finally {
            encoderState.reset();
        }
    }
}
