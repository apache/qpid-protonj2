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

import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Decimal128;
import org.apache.qpid.proton4j.amqp.Decimal32;
import org.apache.qpid.proton4j.amqp.Decimal64;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedByte;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton4j.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;
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

    // TODO - It might be simpler to just convert back and forth from proton-j to new codec
    //        types and just implement comparisons for the new stuff instead of adding comparisons
    //        for the old types.

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

    //----- Handle conversions from new codec types to older proton-j types

    public static Object transcribeToLegacyType(Object newType) {
        Object result = newType;  // For pass-through of types that can map directly

        // Basic Types
        if (newType instanceof UnsignedByte) {
            result = transcribeToLegacyType((UnsignedByte) newType);
        } else if (newType instanceof UnsignedShort) {
            result = transcribeToLegacyType((UnsignedShort) newType);
        } else if (newType instanceof UnsignedInteger) {
            result = transcribeToLegacyType((UnsignedInteger) newType);
        } else if (newType instanceof UnsignedLong) {
            result = transcribeToLegacyType((UnsignedLong) newType);
        } else if (newType instanceof Binary) {
            result = transcribeToLegacyType((Binary) newType);
        } else if (newType instanceof Symbol) {
            result = transcribeToLegacyType((Symbol) newType);
        } else if (newType instanceof Decimal32) {
            result = transcribeToLegacyType((Decimal32) newType);
        } else if (newType instanceof Decimal64) {
            result = transcribeToLegacyType((Decimal64) newType);
        } else if (newType instanceof Decimal128) {
            result = transcribeToLegacyType((Decimal128) newType);
        }

        // Arrays, Maps and Lists
        if (newType instanceof Map) {
            result = transcribeToLegacyType((Map<?, ?>) newType);
        } // TODO

        // Enumerations
        if (newType instanceof Role) {
            result = transcribeToLegacyType((Role) newType);
        } else if (newType instanceof SenderSettleMode) {
            result = transcribeToLegacyType((SenderSettleMode) newType);
        } else if (newType instanceof ReceiverSettleMode) {
            result = transcribeToLegacyType((ReceiverSettleMode) newType);
        } else if (newType instanceof TerminusDurability) {
            result = transcribeToLegacyType((TerminusDurability) newType);
        } else if (newType instanceof TerminusExpiryPolicy) {
            result = transcribeToLegacyType((TerminusExpiryPolicy) newType);
        }

        // Messaging Types

        // Transaction Types

        // Transport Types
        if (newType instanceof Open) {
            result = transcribeToLegacyType((Open) newType);
        } else if (newType instanceof Begin) {
            result = transcribeToLegacyType((Begin) newType);
        } else if (newType instanceof Attach) {
            result = transcribeToLegacyType((Attach) newType);
        }

        // Security Types

        return result;
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param open
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.transport.Open transcribeToLegacyType(Open open) {
        org.apache.qpid.proton.amqp.transport.Open legacyOpen = new org.apache.qpid.proton.amqp.transport.Open();

        legacyOpen.setContainerId(open.getContainerId());
        legacyOpen.setHostname(open.getHostname());
        if (open.getMaxFrameSize() != null) {
            legacyOpen.setMaxFrameSize(org.apache.qpid.proton.amqp.UnsignedInteger.valueOf(open.getMaxFrameSize().intValue()));
        }
        if (open.getChannelMax() != null) {
            legacyOpen.setChannelMax(org.apache.qpid.proton.amqp.UnsignedShort.valueOf(open.getChannelMax().shortValue()));
        }
        if (open.getIdleTimeOut() != null) {
            legacyOpen.setIdleTimeOut(transcribeToLegacyType(open.getIdleTimeOut()));
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
    public static org.apache.qpid.proton.amqp.transport.Begin transcribeToLegacyType(Begin begin) {
        org.apache.qpid.proton.amqp.transport.Begin legacyBegin = new org.apache.qpid.proton.amqp.transport.Begin();

        if (begin.hasHandleMax()) {
            legacyBegin.setHandleMax(org.apache.qpid.proton.amqp.UnsignedInteger.valueOf(begin.getHandleMax()));
        }
        if (begin.hasIncomingWindow()) {
            legacyBegin.setIncomingWindow(org.apache.qpid.proton.amqp.UnsignedInteger.valueOf(begin.getIncomingWindow()));
        }
        if (begin.hasNextOutgoingId()) {
            legacyBegin.setNextOutgoingId(org.apache.qpid.proton.amqp.UnsignedInteger.valueOf(begin.getNextOutgoingId()));
        }
        if (begin.hasOutgoingWindow()) {
            legacyBegin.setOutgoingWindow(org.apache.qpid.proton.amqp.UnsignedInteger.valueOf(begin.getOutgoingWindow()));
        }
        if (begin.hasRemoteChannel()) {
            legacyBegin.setRemoteChannel(org.apache.qpid.proton.amqp.UnsignedShort.valueOf((short) begin.getRemoteChannel()));
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
     * @param attach
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.transport.Attach transcribeToLegacyType(Attach attach) {
        org.apache.qpid.proton.amqp.transport.Attach legacyAttach = new org.apache.qpid.proton.amqp.transport.Attach();

        if (attach.hasName()) {
            legacyAttach.setName(attach.getName());
        }
        if (attach.hasHandle()) {
            legacyAttach.setHandle(org.apache.qpid.proton.amqp.UnsignedInteger.valueOf(attach.getHandle()));
        }
        if (attach.hasRole()) {
            legacyAttach.setRole(transcribeToLegacyType(attach.getRole()));
        }
        if (attach.hasSenderSettleMode()) {
            legacyAttach.setSndSettleMode(transcribeToLegacyType(attach.getSndSettleMode()));
        }
        if (attach.hasReceiverSettleMode()) {
            legacyAttach.setRcvSettleMode(transcribeToLegacyType(attach.getRcvSettleMode()));
        }
        if (attach.hasIncompleteUnsettled()) {
            legacyAttach.setIncompleteUnsettled(attach.getIncompleteUnsettled());
        }
        if (attach.hasOfferedCapabilites()) {
            legacyAttach.setOfferedCapabilities(transcribeToLegacyType(attach.getOfferedCapabilities()));
        }
        if (attach.hasDesiredCapabilites()) {
            legacyAttach.setDesiredCapabilities(transcribeToLegacyType(attach.getDesiredCapabilities()));
        }
        if (attach.hasProperties()) {
            legacyAttach.setProperties(transcribeToLegacyType(attach.getProperties()));
        }
        if (attach.hasInitialDeliveryCount()) {
            legacyAttach.setInitialDeliveryCount(org.apache.qpid.proton.amqp.UnsignedInteger.valueOf(attach.getInitialDeliveryCount()));
        }
        if (attach.hasMaxMessageSize()) {
            legacyAttach.setMaxMessageSize(transcribeToLegacyType(attach.getMaxMessageSize()));
        }

        // TODO private Source source;
        // TODO private Target target;
        // TODO private Map<Binary, DeliveryState> unsettled;

        return legacyAttach;
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param map
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static Map<?, ?> transcribeToLegacyType(Map<?, ?> map) {
        Map<Object, Object> legacySafeMap = new LinkedHashMap<>();

        for (Entry<?, ?> entry : map.entrySet()) {
            legacySafeMap.put(transcribeToLegacyType(entry.getKey()), transcribeToLegacyType(entry.getValue()));
        }

        return legacySafeMap;
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param symbols
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.Symbol[] transcribeToLegacyType(Symbol[] symbols) {
        org.apache.qpid.proton.amqp.Symbol[] legacySymbols = new org.apache.qpid.proton.amqp.Symbol[symbols.length];

        for (int i = 0; i < symbols.length; ++i) {
            legacySymbols[i] = org.apache.qpid.proton.amqp.Symbol.valueOf(symbols[i].toString());
        }

        return legacySymbols;
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param binary
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.Binary transcribeToLegacyType(Binary binary) {
        byte[] copy = new byte[binary.getLength()];
        System.arraycopy(binary.getArray(), binary.getArrayOffset(), copy, 0, copy.length);
        return new org.apache.qpid.proton.amqp.Binary(copy);
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param symbol
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.Symbol transcribeToLegacyType(Symbol symbol) {
        return org.apache.qpid.proton.amqp.Symbol.valueOf(symbol.toString());
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param ubyte
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.UnsignedByte transcribeToLegacyType(UnsignedByte ubyte) {
        return org.apache.qpid.proton.amqp.UnsignedByte.valueOf(ubyte.byteValue());
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param ushort
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.UnsignedShort transcribeToLegacyType(UnsignedShort ushort) {
        return org.apache.qpid.proton.amqp.UnsignedShort.valueOf(ushort.shortValue());
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param uint
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.UnsignedInteger transcribeToLegacyType(UnsignedInteger uint) {
        return org.apache.qpid.proton.amqp.UnsignedInteger.valueOf(uint.intValue());
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param ulong
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.UnsignedLong transcribeToLegacyType(UnsignedLong ulong) {
        return org.apache.qpid.proton.amqp.UnsignedLong.valueOf(ulong.longValue());
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param decimal32
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.Decimal32 transcribeToLegacyType(Decimal32 decimal32) {
        return new org.apache.qpid.proton.amqp.Decimal32(decimal32.intValue());
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param decimal64
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.Decimal64 transcribeToLegacyType(Decimal64 decimal64) {
        return new org.apache.qpid.proton.amqp.Decimal64(decimal64.longValue());
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param decimal128
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.Decimal128 transcribeToLegacyType(Decimal128 decimal128) {
        return new org.apache.qpid.proton.amqp.Decimal128(decimal128.asBytes());
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param terminusDurability
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.messaging.TerminusDurability transcribeToLegacyType(TerminusDurability terminusDurability) {
        return org.apache.qpid.proton.amqp.messaging.TerminusDurability.valueOf(terminusDurability.name());
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param terminusExpiryPolicy
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy transcribeToLegacyType(TerminusExpiryPolicy terminusExpiryPolicy) {
        return org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy.valueOf(terminusExpiryPolicy.name());
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param role
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.transport.Role transcribeToLegacyType(Role role) {
        return org.apache.qpid.proton.amqp.transport.Role.valueOf(role.name());
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param senderSettleMode
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.transport.SenderSettleMode transcribeToLegacyType(SenderSettleMode senderSettleMode) {
        return org.apache.qpid.proton.amqp.transport.SenderSettleMode.valueOf(senderSettleMode.name());
    }

    /**
     * Transcribe a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param receiverSettleMode
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static org.apache.qpid.proton.amqp.transport.ReceiverSettleMode transcribeToLegacyType(ReceiverSettleMode receiverSettleMode) {
        return org.apache.qpid.proton.amqp.transport.ReceiverSettleMode.valueOf(receiverSettleMode.name());
    }
}
