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
package org.apache.qpid.proton4j.codec.legacy;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;

/**
 * Adapter to allow using the legacy proton-j codec in tests for new proton library.
 */
public final class LegacyCodecAdapter {

    private final DecoderImpl decoder = new DecoderImpl();
    private final EncoderImpl encoder = new EncoderImpl(decoder);

    /**
     * Create a codec adapter instance.
     */
    public LegacyCodecAdapter() {
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);
    }

    /**
     * Encode the given type using the legacy codec's Encoder implementation and then
     * transfer the encoded bytes into a {@link ProtonBuffer} which can be used for
     * decoding using the new codec.
     *
     * Usually this method should be passed a legacy type or other primitive value.
     *
     * @param value
     *      The value to be encoded in a {@link ProtonBuffer}.
     *
     * @return a {@link ProtonBuffer} with the encoded bytes ready for reading.
     */
    public ProtonBuffer encodeUsingLegacyEncoder(Object value) {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        ByteBuffer byteBuffer = ByteBuffer.allocate(8192);

        try {
            encoder.setByteBuffer(byteBuffer);
            encoder.writeObject(value);
            byteBuffer.flip();
        } finally {
            encoder.setByteBuffer((ByteBuffer) null);
        }

        buffer.writeBytes(byteBuffer);

        return buffer;
    }

    /**
     * Decode a proper legacy type from the given buffer and return it inside a type
     * adapter that can be used for comparison.
     *
     * @param buffer
     *      The buffer containing the encoded type.
     *
     * @return a {@link LegacyTypeAdapter} that can compare against the new version of the type.
     */
    public LegacyTypeAdapter<?, ?> decodeLegacyType(ProtonBuffer buffer) {
        ByteBuffer byteBuffer = buffer.toByteBuffer();
        Object result = null;

        try {
            decoder.setByteBuffer(byteBuffer);
            result = decoder.readObject();
        } finally {
            decoder.setBuffer(null);
        }

        return LegacyTypeAdapterFactory.createTypeAdapter(result);
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param open
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public org.apache.qpid.proton.amqp.transport.Open transcribeToLegacyType(Open open) {
        org.apache.qpid.proton.amqp.transport.Open legacyOpen = new org.apache.qpid.proton.amqp.transport.Open();

        legacyOpen.setContainerId(open.getContainerId());
        legacyOpen.setHostname(open.getHostname());
        if (open.getMaxFrameSize() != null) {
            legacyOpen.setMaxFrameSize(UnsignedInteger.valueOf(open.getMaxFrameSize().intValue()));
        }
        if (open.getChannelMax() != null) {
            legacyOpen.setChannelMax(UnsignedShort.valueOf(open.getChannelMax().shortValue()));
        }
        if (open.getIdleTimeOut() != null) {
            legacyOpen.setIdleTimeOut(UnsignedInteger.valueOf(open.getIdleTimeOut().intValue()));
        }
        if (open.getOutgoingLocales() != null) {
            legacyOpen.setOutgoingLocales(transcribeToLegacyType(open.getOutgoingLocales()));
        }
        if (open.getIncomingLocales() != null) {
            legacyOpen.setIncomingLocales(transcribeToLegacyType(open.getIncomingLocales()));
        }
        if (open.getOfferedCapabilities() != null) {
            legacyOpen.setOfferedCapabilities(transcribeToLegacyType(open.getOfferedCapabilities()));
        }
        if (open.getDesiredCapabilities() != null) {
            legacyOpen.setDesiredCapabilities(transcribeToLegacyType(open.getDesiredCapabilities()));
        }
        if (open.getProperties() != null) {
            legacyOpen.setProperties(transcribeToLegacyType(open.getProperties()));
        }

        return legacyOpen;
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param begin
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public org.apache.qpid.proton.amqp.transport.Begin transcribeToLegacyType(Begin begin) {
        org.apache.qpid.proton.amqp.transport.Begin legacyBegin = new org.apache.qpid.proton.amqp.transport.Begin();

        if (begin.hasHandleMax()) {
            legacyBegin.setHandleMax(UnsignedInteger.valueOf(begin.getHandleMax()));
        }
        if (begin.hasIncomingWindow()) {
            legacyBegin.setIncomingWindow(UnsignedInteger.valueOf(begin.getIncomingWindow()));
        }
        if (begin.hasNextOutgoingId()) {
            legacyBegin.setNextOutgoingId(UnsignedInteger.valueOf(begin.getNextOutgoingId()));
        }
        if (begin.hasOutgoingWindow()) {
            legacyBegin.setOutgoingWindow(UnsignedInteger.valueOf(begin.getOutgoingWindow()));
        }
        if (begin.hasRemoteChannel()) {
            legacyBegin.setRemoteChannel(UnsignedShort.valueOf((short) begin.getRemoteChannel()));
        }
        if (begin.hasOfferedCapabilites()) {
            legacyBegin.setOfferedCapabilities(transcribeToLegacyType(begin.getOfferedCapabilities()));
        }
        if (begin.hasDesiredCapabilites()) {
            legacyBegin.setDesiredCapabilities(transcribeToLegacyType(begin.getDesiredCapabilities()));
        }
        if (begin.hasProperties()) {
            legacyBegin.setProperties(transcribeToLegacyType(begin.getProperties()));
        }

        return legacyBegin;
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param properties
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public Map<org.apache.qpid.proton.amqp.Symbol, Object> transcribeToLegacyType(Map<Symbol, Object> properties) {
        Map<org.apache.qpid.proton.amqp.Symbol, Object> legacyProperties = new LinkedHashMap<>();

        for (Entry<Symbol, Object> entry : properties.entrySet()) {
            legacyProperties.put(org.apache.qpid.proton.amqp.Symbol.valueOf(entry.getKey().toString()), entry.getValue());
        }

        return legacyProperties;
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param symbols
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public org.apache.qpid.proton.amqp.Symbol[] transcribeToLegacyType(Symbol[] symbols) {
        org.apache.qpid.proton.amqp.Symbol[] legacySymbols = new org.apache.qpid.proton.amqp.Symbol[symbols.length];

        for (int i = 0; i < symbols.length; ++i) {
            legacySymbols[i] = org.apache.qpid.proton.amqp.Symbol.valueOf(symbols[i].toString());
        }

        return legacySymbols;
    }
}
