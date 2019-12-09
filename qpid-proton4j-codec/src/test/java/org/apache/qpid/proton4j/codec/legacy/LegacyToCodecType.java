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
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.DeleteOnClose;
import org.apache.qpid.proton4j.amqp.messaging.DeleteOnNoLinks;
import org.apache.qpid.proton4j.amqp.messaging.DeleteOnNoLinksOrMessages;
import org.apache.qpid.proton4j.amqp.messaging.DeleteOnNoMessages;
import org.apache.qpid.proton4j.amqp.messaging.LifetimePolicy;
import org.apache.qpid.proton4j.amqp.messaging.Modified;
import org.apache.qpid.proton4j.amqp.messaging.Outcome;
import org.apache.qpid.proton4j.amqp.messaging.Received;
import org.apache.qpid.proton4j.amqp.messaging.Rejected;
import org.apache.qpid.proton4j.amqp.messaging.Released;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton4j.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton4j.amqp.transactions.Declared;
import org.apache.qpid.proton4j.amqp.transactions.TransactionalState;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Close;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.End;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
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
        } // TODO - Convert Lists with legacy types to new codec types.

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
        if (legacyType instanceof org.apache.qpid.proton.amqp.messaging.Outcome) {
            return convertToCodecType((org.apache.qpid.proton.amqp.messaging.Outcome) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.messaging.Source) {
            return convertToCodecType((org.apache.qpid.proton.amqp.messaging.Source) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.messaging.Target) {
            return convertToCodecType((org.apache.qpid.proton.amqp.messaging.Target) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.messaging.LifetimePolicy) {
            return convertToCodecType((org.apache.qpid.proton.amqp.messaging.LifetimePolicy) legacyType);
        }

        // Transaction Types
        // TODO - Transaction types converted to new codec types

        // Transport Types
        if (legacyType instanceof org.apache.qpid.proton.amqp.transport.Open) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.Open) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.transport.Close) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.Close) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.transport.Begin) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.Begin) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.transport.End) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.End) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.transport.Attach) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.Attach) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.transport.Detach) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.Detach) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.transport.ErrorCondition) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.ErrorCondition) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.transport.DeliveryState) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.DeliveryState) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.transport.Source) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.Source) legacyType);
        } else if (legacyType instanceof org.apache.qpid.proton.amqp.transport.Target) {
            return convertToCodecType((org.apache.qpid.proton.amqp.transport.Target) legacyType);
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
            newOpen.setMaxFrameSize(legacyOpen.getMaxFrameSize().longValue());
        }
        if (legacyOpen.getChannelMax() != null) {
            newOpen.setChannelMax(legacyOpen.getChannelMax().intValue());
        }
        if (legacyOpen.getIdleTimeOut() != null) {
            newOpen.setIdleTimeOut(legacyOpen.getIdleTimeOut().longValue());
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
     * @param legacyClose
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static Close convertToCodecType(org.apache.qpid.proton.amqp.transport.Close legacyClose) {
        Close close = new Close();

        if (legacyClose.getError() != null) {
            close.setError(convertToCodecType(legacyClose.getError()));
        }

        return close;
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
     * @param legacyEnd
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static End convertToCodecType(org.apache.qpid.proton.amqp.transport.End legacyEnd) {
        End end = new End();

        if (legacyEnd.getError() != null) {
            end.setError(convertToCodecType(legacyEnd.getError()));
        }

        return end;
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
            attach.setSenderSettleMode(convertToCodecType(legacyAttach.getSndSettleMode()));
        }
        if (legacyAttach.getRcvSettleMode() != null) {
            attach.setReceiverSettleMode(convertToCodecType(legacyAttach.getRcvSettleMode()));
        }
        attach.setIncompleteUnsettled(legacyAttach.getIncompleteUnsettled());
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
        if (legacyAttach.getSource() != null) {
            attach.setSource(convertToCodecType(legacyAttach.getSource()));
        }
        if (legacyAttach.getTarget() != null) {
            attach.setTarget(convertToCodecType(legacyAttach.getTarget()));
        }
        if (legacyAttach.getUnsettled() != null) {
            attach.setUnsettled((Map<Binary, DeliveryState>) convertToCodecType(legacyAttach.getUnsettled()));
        }

        return attach;
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param legacyDetach
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static Detach convertToCodecType(org.apache.qpid.proton.amqp.transport.Detach legacyDetach) {
        Detach close = new Detach();

        close.setClosed(legacyDetach.getClosed());
        if (legacyDetach.getError() != null) {
            close.setError(convertToCodecType(legacyDetach.getError()));
        }
        if (legacyDetach.getHandle() != null) {
            close.setHandle(legacyDetach.getHandle().longValue());
        }

        return close;
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param legacySource
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static Source convertToCodecType(org.apache.qpid.proton.amqp.transport.Source legacySource) {
        return convertToCodecType((org.apache.qpid.proton.amqp.messaging.Source) legacySource);
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param legacyTarget
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    public static Target convertToCodecType(org.apache.qpid.proton.amqp.transport.Target legacyTarget) {
        return convertToCodecType((org.apache.qpid.proton.amqp.messaging.Target) legacyTarget);
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param legacySource
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    @SuppressWarnings("unchecked")
    public static Source convertToCodecType(org.apache.qpid.proton.amqp.messaging.Source legacySource) {
        Source source = new Source();

        if (legacySource.getAddress() != null) {
            source.setAddress(legacySource.getAddress());
        }
        if (legacySource.getDurable() != null) {
            source.setDurable(convertToCodecType(legacySource.getDurable()));
        }
        if (legacySource.getExpiryPolicy() != null) {
            source.setExpiryPolicy(convertToCodecType(legacySource.getExpiryPolicy()));
        }
        if (legacySource.getTimeout() != null) {
            source.setTimeout(convertToCodecType(legacySource.getTimeout()));
        }
        source.setDynamic(legacySource.getDynamic());
        if (legacySource.getDynamicNodeProperties() != null) {
            source.setDynamicNodeProperties((Map<Symbol, Object>) convertToCodecType(legacySource.getDynamicNodeProperties()));
        }
        if (legacySource.getDistributionMode() != null) {
            source.setDistributionMode(convertToCodecType(legacySource.getDistributionMode()));
        }
        if (legacySource.getFilter() != null) {
            source.setFilter((Map<Symbol, Object>) convertToCodecType(legacySource.getFilter()));
        }
        if (legacySource.getDefaultOutcome() != null) {
            source.setDefaultOutcome(convertToCodecType(legacySource.getDefaultOutcome()));
        }
        if (legacySource.getOutcomes() != null) {
            source.setOutcomes(convertToCodecType(legacySource.getOutcomes()));
        }
        if (legacySource.getCapabilities() != null) {
            source.setCapabilities(convertToCodecType(legacySource.getCapabilities()));
        }

        return source;
    }

    /**
     * convert a legacy type to a new codec type for encoding or other operation that requires a new type.
     *
     * @param legacyTarget
     *      The legacy type to be converted into a new codec equivalent type.
     *
     * @return the new codec version of the legacy type.
     */
    @SuppressWarnings("unchecked")
    public static Target convertToCodecType(org.apache.qpid.proton.amqp.messaging.Target legacyTarget) {
        Target target = new Target();

        if (legacyTarget.getAddress() != null) {
            target.setAddress(legacyTarget.getAddress());
        }
        if (legacyTarget.getDurable() != null) {
            target.setDurable(convertToCodecType(legacyTarget.getDurable()));
        }
        if (legacyTarget.getExpiryPolicy() != null) {
            target.setExpiryPolicy(convertToCodecType(legacyTarget.getExpiryPolicy()));
        }
        if (legacyTarget.getTimeout() != null) {
            target.setTimeout(convertToCodecType(legacyTarget.getTimeout()));
        }
        legacyTarget.setDynamic(legacyTarget.getDynamic());
        if (legacyTarget.getDynamicNodeProperties() != null) {
            target.setDynamicNodeProperties((Map<Symbol, Object>) convertToCodecType(legacyTarget.getDynamicNodeProperties()));
        }
        if (legacyTarget.getCapabilities() != null) {
            target.setCapabilities(convertToCodecType(legacyTarget.getCapabilities()));
        }

        return target;
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

    /**
     * convert a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param errorCondition
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    @SuppressWarnings("unchecked")
    public static ErrorCondition convertToCodecType(org.apache.qpid.proton.amqp.transport.ErrorCondition errorCondition) {
        Symbol condition = null;
        String description = null;
        Map<Symbol, Object> info = null;

        if (errorCondition.getCondition() != null) {
            condition = convertToCodecType(errorCondition.getCondition());
        }
        if (errorCondition.getDescription() != null) {
            description = errorCondition.getDescription();
        }
        if (errorCondition.getInfo() != null) {
            info = (Map<Symbol, Object>) convertToCodecType(errorCondition.getInfo());
        }

        return new ErrorCondition(condition, description, info);
    }

    /**
     * convert a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param state
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    @SuppressWarnings("unchecked")
    public static DeliveryState convertToCodecType(org.apache.qpid.proton.amqp.transport.DeliveryState state) {
        if (state instanceof org.apache.qpid.proton.amqp.messaging.Accepted) {
            return Accepted.getInstance();
        } else if (state instanceof org.apache.qpid.proton.amqp.messaging.Rejected) {
            Rejected rejected = new Rejected();
            rejected.setError(convertToCodecType(((org.apache.qpid.proton.amqp.messaging.Rejected) state).getError()));
            return rejected;
        } else if (state instanceof org.apache.qpid.proton.amqp.messaging.Released) {
            return Released.getInstance();
        } else if (state instanceof org.apache.qpid.proton.amqp.messaging.Modified) {
            Modified modified = new Modified();
            modified.setDeliveryFailed(((org.apache.qpid.proton.amqp.messaging.Modified) state).getDeliveryFailed());
            modified.setMessageAnnotations((Map<Symbol, Object>) convertToCodecType(((org.apache.qpid.proton.amqp.messaging.Modified) state).getMessageAnnotations()));
            modified.setUndeliverableHere(((org.apache.qpid.proton.amqp.messaging.Modified) state).getUndeliverableHere());
            return modified;
        } else if (state instanceof org.apache.qpid.proton.amqp.messaging.Received) {
            Received received = new Received();
            received.setSectionOffset(convertToCodecType(((org.apache.qpid.proton.amqp.messaging.Received) state).getSectionOffset()));
            received.setSectionNumber(convertToCodecType(((org.apache.qpid.proton.amqp.messaging.Received) state).getSectionNumber()));
            return received;
        } else if (state instanceof org.apache.qpid.proton.amqp.transaction.Declared) {
            Declared declared = new Declared();
            declared.setTxnId(convertToCodecType(((org.apache.qpid.proton.amqp.transaction.Declared) state).getTxnId()));
            return declared;
        } else if (state instanceof org.apache.qpid.proton.amqp.transaction.TransactionalState) {
            TransactionalState txState = new TransactionalState();
            txState.setOutcome(convertToCodecType(((org.apache.qpid.proton.amqp.transaction.TransactionalState) state).getOutcome()));
            txState.setTxnId(convertToCodecType(((org.apache.qpid.proton.amqp.transaction.TransactionalState) state).getTxnId()));
            return txState;
        }

        return null;
    }

    /**
     * convert a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param outcome
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    @SuppressWarnings("unchecked")
    public static Outcome convertToCodecType(org.apache.qpid.proton.amqp.messaging.Outcome outcome) {
        if (outcome instanceof org.apache.qpid.proton.amqp.messaging.Accepted) {
            return Accepted.getInstance();
        } else if (outcome instanceof org.apache.qpid.proton.amqp.messaging.Rejected) {
            Rejected rejected = new Rejected();
            rejected.setError(convertToCodecType(((org.apache.qpid.proton.amqp.messaging.Rejected) outcome).getError()));
            return rejected;
        } else if (outcome instanceof org.apache.qpid.proton.amqp.messaging.Released) {
            return Released.getInstance();
        } else if (outcome instanceof org.apache.qpid.proton.amqp.messaging.Modified) {
            Modified modified = new Modified();
            modified.setDeliveryFailed(((org.apache.qpid.proton.amqp.messaging.Modified) outcome).getDeliveryFailed());
            modified.setMessageAnnotations((Map<Symbol, Object>) convertToCodecType(((org.apache.qpid.proton.amqp.messaging.Modified) outcome).getMessageAnnotations()));
            modified.setUndeliverableHere(((org.apache.qpid.proton.amqp.messaging.Modified) outcome).getUndeliverableHere());
            return modified;
        }

        return null;
    }

    /**
     * convert a new Codec type to a legacy type for encoding or other operation that requires a legacy type.
     *
     * @param policy
     *      The new codec type to be converted to the legacy codec version
     *
     * @return the legacy version of the new type.
     */
    public static LifetimePolicy convertToCodecType(org.apache.qpid.proton.amqp.messaging.LifetimePolicy policy) {
        LifetimePolicy legacyPolicy = null;

        if (policy instanceof org.apache.qpid.proton.amqp.messaging.DeleteOnClose) {
            legacyPolicy = DeleteOnClose.getInstance();
        } else if (policy instanceof org.apache.qpid.proton.amqp.messaging.DeleteOnNoLinks) {
            legacyPolicy = DeleteOnNoLinks.getInstance();
        } else if (policy instanceof org.apache.qpid.proton.amqp.messaging.DeleteOnNoLinksOrMessages) {
            legacyPolicy = DeleteOnNoLinksOrMessages.getInstance();
        } else if (policy instanceof org.apache.qpid.proton.amqp.messaging.DeleteOnNoMessages) {
            legacyPolicy = DeleteOnNoMessages.getInstance();
        }

        return legacyPolicy;
    }

}
