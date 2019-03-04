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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

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

/**
 * Converts from Legacy AMQP types to the new codec types to allow for cross testing
 * using the legacy codec and then having new types to compare results.
 */
public abstract class LegacyToCodecType {

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param legacyType
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static Object convertToCodecType(Object legacyType) {

        // Basic Types
        if (legacyType instanceof org.apache.qpid.proton.amqp.UnsignedByte) {
            return convertToCodecType((org.apache.qpid.proton.amqp.UnsignedByte) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.UnsignedShort) {
            return convertToCodecType((org.apache.qpid.proton.amqp.UnsignedShort) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.UnsignedInteger) {
            return convertToCodecType((org.apache.qpid.proton.amqp.UnsignedInteger) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.UnsignedLong) {
            return convertToCodecType((org.apache.qpid.proton.amqp.UnsignedLong) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.Binary) {
            return convertToCodecType((org.apache.qpid.proton.amqp.Binary) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.Symbol) {
            return convertToCodecType((org.apache.qpid.proton.amqp.Symbol) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.Decimal32) {
            return convertToCodecType((org.apache.qpid.proton.amqp.Decimal32) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.Decimal64) {
            return convertToCodecType((org.apache.qpid.proton.amqp.Decimal64) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.Decimal128) {
            return convertToCodecType((org.apache.qpid.proton.amqp.Decimal128) legacyType);
        }

        // Arrays, Maps and Lists
        if (legacyType instanceof Map) {
            return convertToCodecType((Map<?, ?>) legacyType);
        } // TODO

        // Enumerations
        if (legacyType instanceof org.apache.qpid.proton.amqp.transport.Role) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.Role) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.transport.SenderSettleMode) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.SenderSettleMode) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.transport.ReceiverSettleMode) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.ReceiverSettleMode) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.messaging.TerminusDurability) {
            return convertToCodecType((org.apache.qpid.proton.amqp.messaging.TerminusDurability) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy) {
            return convertToCodecType((org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy) legacyType);
        }

        // Messaging Types

        // Transaction Types

        // Transport Types
        if (legacyType instanceof org.apache.qpid.proton.amqp.transport.Open) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.Open) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.transport.Begin) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.Begin) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.transport.Attach) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.Attach) legacyType);
        }

        // Security Types

        return legacyType;  // Pass through the type as we don't know how to convert it.
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param legacyOpen
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    @SuppressWarnings("unchecked")
    public static Open convertToCodecType(org.apache.qpid.proton.amqp.transport.Open legacyOpen) {
        Open newOpen = new Open();

        newOpen.setContainerId(legacyOpen.getContainerId());
        newOpen.setHostname(legacyOpen.getHostname());
        if (legacyOpen.getMaxFrameSize() != null) {
            newOpen.setMaxFrameSize(convertToCodecType(legacyOpen.getMaxFrameSize()));
        }
        if (legacyOpen.getChannelMax() != null) {
            newOpen.setChannelMax(convertToCodecType(legacyOpen.getChannelMax()));
        }
        if (legacyOpen.getIdleTimeOut() != null) {
            newOpen.setIdleTimeOut(convertToCodecType(legacyOpen.getIdleTimeOut()));
        }
        if (legacyOpen.getOutgoingLocales() != null) {
            newOpen.setOutgoingLocales(convertToCodecType(legacyOpen.getOutgoingLocales()));
        }
        if (legacyOpen.getIncomingLocales() != null) {
            newOpen.setIncomingLocales(convertToCodecType(legacyOpen.getIncomingLocales()));
        }
        if (legacyOpen.getOfferedCapabilities() != null) {
            newOpen.setOfferedCapabilities(convertToCodecType(legacyOpen.getOfferedCapabilities()));
        }
        if (legacyOpen.getDesiredCapabilities() != null) {
            newOpen.setDesiredCapabilities(convertToCodecType(legacyOpen.getDesiredCapabilities()));
        }
        if (legacyOpen.getProperties() != null) {
            newOpen.setProperties((Map<Symbol, Object>) convertToCodecType(legacyOpen.getProperties()));
        }

        return newOpen;
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param legacyBegin
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    @SuppressWarnings("unchecked")
    public static Begin convertToCodecType(org.apache.qpid.proton.amqp.transport.Begin legacyBegin) {
        Begin begin = new Begin();

        if (legacyBegin.getHandleMax() != null) {
            begin.setHandleMax(legacyBegin.getHandleMax().longValue());
        }
        if (legacyBegin.getIncomingWindow() != null) {
            begin.setIncomingWindow(legacyBegin.getIncomingWindow().longValue());
        }
        if (legacyBegin.getNextOutgoingId() != null) {
            begin.setNextOutgoingId(legacyBegin.getNextOutgoingId().longValue());
        }
        if (legacyBegin.getOutgoingWindow() != null) {
            begin.setOutgoingWindow(legacyBegin.getOutgoingWindow().longValue());
        }
        if (legacyBegin.getRemoteChannel() != null) {
            begin.setRemoteChannel(legacyBegin.getRemoteChannel().intValue());
        }
        if (legacyBegin.getOfferedCapabilities() != null) {
            begin.setOfferedCapabilities(convertToCodecType(legacyBegin.getOfferedCapabilities()));
        }
        if (legacyBegin.getDesiredCapabilities() != null) {
            begin.setDesiredCapabilities(convertToCodecType(legacyBegin.getDesiredCapabilities()));
        }
        if (legacyBegin.getProperties() != null) {
            begin.setProperties((Map<Symbol, Object>) convertToCodecType(legacyBegin.getProperties()));
        }

        return begin;
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param legacyAttach
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    @SuppressWarnings("unchecked")
    public static Attach convertToCodecType(org.apache.qpid.proton.amqp.transport.Attach legacyAttach) {
        Attach attach = new Attach();

        if (legacyAttach.getName() != null) {
            attach.setName(legacyAttach.getName());
        }
        if (legacyAttach.getHandle() != null) {
            attach.setHandle(legacyAttach.getHandle().longValue());
        }
        if (legacyAttach.getRole() != null) {
            attach.setRole(convertToCodecType(legacyAttach.getRole()));
        }
        if (legacyAttach.getSndSettleMode() != null) {
            attach.setSndSettleMode(convertToCodecType(legacyAttach.getSndSettleMode()));
        }
        if (legacyAttach.getRcvSettleMode() != null) {
            attach.setRcvSettleMode(convertToCodecType(legacyAttach.getRcvSettleMode()));
        }
        if (legacyAttach.getIncompleteUnsettled()) {
            attach.setIncompleteUnsettled(legacyAttach.getIncompleteUnsettled());
        }
        if (legacyAttach.getOfferedCapabilities() != null) {
            attach.setOfferedCapabilities(convertToCodecType(legacyAttach.getOfferedCapabilities()));
        }
        if (legacyAttach.getDesiredCapabilities() != null) {
            attach.setDesiredCapabilities(convertToCodecType(legacyAttach.getDesiredCapabilities()));
        }
        if (legacyAttach.getProperties() != null) {
            attach.setProperties((Map<Symbol, Object>) convertToCodecType(legacyAttach.getProperties()));
        }
        if (legacyAttach.getInitialDeliveryCount() != null) {
            attach.setInitialDeliveryCount(legacyAttach.getInitialDeliveryCount().longValue());
        }
        if (legacyAttach.getMaxMessageSize() != null) {
            attach.setMaxMessageSize(convertToCodecType(legacyAttach.getMaxMessageSize()));
        }

        // TODO private Source source;
        // TODO private Target target;
        // TODO private Map<Binary, DeliveryState> unsettled;

        return attach;
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param map
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static Map<?, ?> convertToCodecType(Map<?, ?> map) {
        Map<Object, Object> legacySafeMap = new LinkedHashMap<>();

        for (Entry<?, ?> entry : map.entrySet()) {
            legacySafeMap.put(convertToCodecType(entry.getKey()), convertToCodecType(entry.getValue()));
        }

        return legacySafeMap;
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param symbols
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static Symbol[] convertToCodecType(org.apache.qpid.proton.amqp.Symbol[] symbols) {
        Symbol[] array = new Symbol[symbols.length];

        for (int i = 0; i < symbols.length; ++i) {
            array[i] = Symbol.valueOf(symbols[i].toString());
        }

        return array;
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param binary
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static Binary convertToCodecType(org.apache.qpid.proton.amqp.Binary binary) {
        byte[] copy = new byte[binary.getLength()];
        System.arraycopy(binary.getArray(), binary.getArrayOffset(), copy, 0, copy.length);
        return new Binary(copy);
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param symbol
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static Symbol convertToCodecType(org.apache.qpid.proton.amqp.Symbol symbol) {
        return Symbol.valueOf(symbol.toString());
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param ubyte
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static UnsignedByte convertToCodecType(org.apache.qpid.proton.amqp.UnsignedByte ubyte) {
        return UnsignedByte.valueOf(ubyte.byteValue());
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param ushort
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static UnsignedShort convertToCodecType(org.apache.qpid.proton.amqp.UnsignedShort ushort) {
        return UnsignedShort.valueOf(ushort.shortValue());
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param uint
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static UnsignedInteger convertToCodecType(org.apache.qpid.proton.amqp.UnsignedInteger uint) {
        return UnsignedInteger.valueOf(uint.intValue());
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param ulong
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static UnsignedLong convertToCodecType(org.apache.qpid.proton.amqp.UnsignedLong ulong) {
        return UnsignedLong.valueOf(ulong.longValue());
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param decimal32
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static Decimal32 convertToCodecType(org.apache.qpid.proton.amqp.Decimal32 decimal32) {
        return new Decimal32(decimal32.intValue());
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param decimal64
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static Decimal64 convertToCodecType(org.apache.qpid.proton.amqp.Decimal64 decimal64) {
        return new Decimal64(decimal64.longValue());
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param decimal128
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static Decimal128 convertToCodecType(org.apache.qpid.proton.amqp.Decimal128 decimal128) {
        return new Decimal128(decimal128.asBytes());
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param terminusDurability
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static TerminusDurability convertToCodecType(org.apache.qpid.proton.amqp.messaging.TerminusDurability terminusDurability) {
        return TerminusDurability.valueOf(terminusDurability.name());
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param terminusExpiryPolicy
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static TerminusExpiryPolicy convertToCodecType(org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy terminusExpiryPolicy) {
        return TerminusExpiryPolicy.valueOf(terminusExpiryPolicy.name());
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param role
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static Role convertToCodecType(org.apache.qpid.proton.amqp.transport.Role role) {
        return Role.valueOf(role.name());
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param senderSettleMode
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static SenderSettleMode convertToCodecType(org.apache.qpid.proton.amqp.transport.SenderSettleMode senderSettleMode) {
        return SenderSettleMode.valueOf(senderSettleMode.name());
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param receiverSettleMode
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static ReceiverSettleMode convertToCodecType(org.apache.qpid.proton.amqp.transport.ReceiverSettleMode receiverSettleMode) {
        return ReceiverSettleMode.valueOf(receiverSettleMode.name());
    }
}
