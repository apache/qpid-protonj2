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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.Attach;
import org.apache.qpid.proton.amqp.transport.Begin;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.Open;

/**
 * Support methods for working with the legacy codec and new proton4j codec
 */
public abstract class LegacyCodecSupport {

    /**
     * Compare a legacy Open to another legacy Open instance.
     *
     * @param legacyOpen1
     *      Array of {@link Open} instances or null
     * @param legacyOpen2
     *      Array of {@link Open} instances or null.
     *
     * @return true if the legacy types are equal.
     */
    public static boolean areEqual(Open legacyOpen1, Open legacyOpen2) {
        if (legacyOpen1 == null && legacyOpen2 == null) {
            return true;
        } else if (legacyOpen1 == null || legacyOpen2 == null) {
            return false;
        }

        if (legacyOpen1.getChannelMax() == null) {
            if (legacyOpen2.getChannelMax() != null) {
                return false;
            }
        } else if (!legacyOpen1.getChannelMax().equals(legacyOpen2.getChannelMax())) {
            return false;
        }

        if (legacyOpen1.getContainerId() == null) {
            if (legacyOpen2.getContainerId() != null) {
                return false;
            }
        } else if (!legacyOpen1.getContainerId().equals(legacyOpen2.getContainerId())) {
            return false;
        }

        if (legacyOpen1.getHostname() == null) {
            if (legacyOpen2.getHostname() != null) {
                return false;
            }
        } else if (!legacyOpen1.getHostname().equals(legacyOpen2.getHostname())) {
            return false;
        }

        if (legacyOpen1.getIdleTimeOut() == null) {
            if (legacyOpen2.getIdleTimeOut() != null) {
                return false;
            }
        } else if (!legacyOpen1.getIdleTimeOut().equals(legacyOpen2.getIdleTimeOut())) {
            return false;
        }

        if (legacyOpen1.getMaxFrameSize() == null) {
            if (legacyOpen2.getMaxFrameSize() != null) {
                return false;
            }
        } else if (!legacyOpen1.getMaxFrameSize().equals(legacyOpen2.getMaxFrameSize())) {
            return false;
        }

        if (legacyOpen1.getProperties() == null) {
            if (legacyOpen2.getProperties() != null) {
                return false;
            }
        } else if (!legacyOpen1.getProperties().equals(legacyOpen2.getProperties())) {
            return false;
        }

        if (!Arrays.equals(legacyOpen1.getDesiredCapabilities(), legacyOpen2.getDesiredCapabilities())) {
            return false;
        }
        if (!Arrays.equals(legacyOpen1.getOfferedCapabilities(), legacyOpen2.getOfferedCapabilities())) {
            return false;
        }
        if (!Arrays.equals(legacyOpen1.getIncomingLocales(), legacyOpen2.getIncomingLocales())) {
            return false;
        }
        if (!Arrays.equals(legacyOpen1.getOutgoingLocales(), legacyOpen2.getOutgoingLocales())) {
            return false;
        }

        return true;
    }

    /**
     * Compare a legacy Open to an Open instance using the proton4j Open type
     *
     * @param newOpen
     *      Array of {@link org.apache.qpid.proton4j.amqp.transport.Open} instances or null
     * @param legacyOpen
     *      Array of {@link Open} instances or null.
     *
     * @return true if the legacy and new types are equal.
     */
    public static boolean areEqual(org.apache.qpid.proton4j.amqp.transport.Open newOpen, Open legacyOpen) {
        return areEqual(legacyOpen, newOpen);
    }

    /**
     * Compare a legacy Open to an Open instance using the proton4j Open type
     *
     * @param legacyOpen
     *      Array of {@link Open} instances or null.
     * @param newOpen
     *      Array of {@link org.apache.qpid.proton4j.amqp.transport.Open} instances or null
     *
     * @return true if the legacy and new types are equal.
     */
    @SuppressWarnings("unchecked")
    public static boolean areEqual(Open legacyOpen, org.apache.qpid.proton4j.amqp.transport.Open newOpen) {
        if (legacyOpen == null && newOpen == null) {
            return true;
        } else if (legacyOpen == null || newOpen == null) {
            return false;
        }

        if (legacyOpen.getChannelMax() == null) {
            if (newOpen.getChannelMax() != null) {
                return false;
            }
        } else if (legacyOpen.getChannelMax().intValue() != newOpen.getChannelMax().intValue()) {
            return false;
        }

        if (legacyOpen.getContainerId() == null) {
            if (newOpen.getContainerId() != null) {
                return false;
            }
        } else if (!legacyOpen.getContainerId().equals(newOpen.getContainerId())) {
            return false;
        }

        if (legacyOpen.getHostname() == null) {
            if (newOpen.getHostname() != null) {
                return false;
            }
        } else if (!legacyOpen.getHostname().equals(newOpen.getHostname())) {
            return false;
        }

        if (legacyOpen.getIdleTimeOut() == null) {
            if (newOpen.getIdleTimeOut() != null) {
                return false;
            }
        } else if (legacyOpen.getIdleTimeOut().longValue() != newOpen.getIdleTimeOut().longValue()) {
            return false;
        }

        if (legacyOpen.getMaxFrameSize() == null) {
            if (newOpen.getMaxFrameSize() != null) {
                return false;
            }
        } else if (legacyOpen.getMaxFrameSize().longValue() != newOpen.getMaxFrameSize().longValue()) {
            return false;
        }

        if (!LegacyCodecSupport.areSymbolKeyMapsEqual(legacyOpen.getProperties(), newOpen.getProperties())) {
            return false;
        }
        if (!LegacyCodecSupport.areEqual(legacyOpen.getDesiredCapabilities(), newOpen.getDesiredCapabilities())) {
            return false;
        }
        if (!LegacyCodecSupport.areEqual(legacyOpen.getOfferedCapabilities(), newOpen.getOfferedCapabilities())) {
            return false;
        }
        if (!LegacyCodecSupport.areEqual(legacyOpen.getIncomingLocales(), newOpen.getIncomingLocales())) {
            return false;
        }
        if (!LegacyCodecSupport.areEqual(legacyOpen.getOutgoingLocales(), newOpen.getOutgoingLocales())) {
            return false;
        }

        return true;
    }

    /**
     * Compare a legacy Begin to another legacy Begin instance.
     *
     * @param legacyType1
     *      Array of {@link Begin} instances or null
     * @param legacyType2
     *      Array of {@link Begin} instances or null.
     *
     * @return true if the legacy types are equal.
     */
    public static boolean areEqual(Begin legacyType1, Begin legacyType2) {
        if (legacyType1 == null && legacyType2 == null) {
            return true;
        } else if (legacyType1 == null || legacyType2 == null) {
            return false;
        }

        if (legacyType1.getHandleMax() == null) {
            if (legacyType2.getHandleMax() != null) {
                return false;
            }
        } else if (!legacyType1.getHandleMax().equals(legacyType2.getHandleMax())) {
            return false;
        }
        if (legacyType1.getIncomingWindow() == null) {
            if (legacyType2.getIncomingWindow() != null) {
                return false;
            }
        } else if (!legacyType1.getIncomingWindow().equals(legacyType2.getIncomingWindow())) {
            return false;
        }
        if (legacyType1.getNextOutgoingId() == null) {
            if (legacyType2.getNextOutgoingId() != null) {
                return false;
            }
        } else if (!legacyType1.getNextOutgoingId().equals(legacyType2.getNextOutgoingId())) {
            return false;
        }
        if (legacyType1.getOutgoingWindow() == null) {
            if (legacyType2.getOutgoingWindow() != null) {
                return false;
            }
        } else if (!legacyType1.getOutgoingWindow().equals(legacyType2.getOutgoingWindow())) {
            return false;
        }
        if (legacyType1.getRemoteChannel() == null) {
            if (legacyType2.getNextOutgoingId() != null) {
                return false;
            }
        } else if (!legacyType1.getRemoteChannel().equals(legacyType2.getRemoteChannel())) {
            return false;
        }
        if (legacyType1.getProperties() == null) {
            if (legacyType2.getProperties() != null) {
                return false;
            }
        } else if (!legacyType1.getProperties().equals(legacyType2.getProperties())) {
            return false;
        }
        if (!Arrays.equals(legacyType1.getDesiredCapabilities(), legacyType2.getDesiredCapabilities())) {
            return false;
        }
        if (!Arrays.equals(legacyType1.getOfferedCapabilities(), legacyType2.getOfferedCapabilities())) {
            return false;
        }

        return true;
    }

    /**
     * Compare a legacy Begin to an Begin instance using the proton4j Begin type
     *
     * @param newBegin
     *      Array of {@link org.apache.qpid.proton4j.amqp.transport.Begin} instances or null
     * @param legacyBegin
     *      Array of {@link Begin} instances or null.
     *
     * @return true if the legacy and new types are equal.
     */
    public static boolean areEqual(org.apache.qpid.proton4j.amqp.transport.Begin newBegin, Begin legacyBegin) {
        return areEqual(legacyBegin, newBegin);
    }

    /**
     * Compare a legacy Begin to an Begin instance using the proton4j Begin type
     *
     * @param legacyBegin
     *      Array of {@link Begin} instances or null.
     * @param newBegin
     *      Array of {@link org.apache.qpid.proton4j.amqp.transport.Begin} instances or null
     *
     * @return true if the legacy and new types are equal.
     */
    @SuppressWarnings("unchecked")
    public static boolean areEqual(Begin legacyBegin, org.apache.qpid.proton4j.amqp.transport.Begin newBegin) {
        if (legacyBegin == null && newBegin == null) {
            return true;
        } else if (legacyBegin == null || newBegin == null) {
            return false;
        }

        if (legacyBegin.getHandleMax() == null) {
            if (!newBegin.hasHandleMax()) {
                return false;
            }
        } else if (legacyBegin.getHandleMax().longValue() != newBegin.getHandleMax()) {
            return false;
        }
        if (legacyBegin.getIncomingWindow() == null) {
            if (!newBegin.hasIncomingWindow()) {
                return false;
            }
        } else if (legacyBegin.getIncomingWindow().longValue() != newBegin.getIncomingWindow()) {
            return false;
        }
        if (legacyBegin.getNextOutgoingId() == null) {
            if (!newBegin.hasNextOutgoingId()) {
                return false;
            }
        } else if (legacyBegin.getNextOutgoingId().longValue() != newBegin.getNextOutgoingId()) {
            return false;
        }
        if (legacyBegin.getOutgoingWindow() == null) {
            if (!newBegin.hasOutgoingWindow()) {
                return false;
            }
        } else if (legacyBegin.getOutgoingWindow().longValue() != newBegin.getOutgoingWindow()) {
            return false;
        }
        if (legacyBegin.getRemoteChannel() == null) {
            if (!newBegin.hasRemoteChannel()) {
                return false;
            }
        } else if (legacyBegin.getRemoteChannel().longValue() != newBegin.getRemoteChannel()) {
            return false;
        }
        if (!LegacyCodecSupport.areSymbolKeyMapsEqual(legacyBegin.getProperties(), newBegin.getProperties())) {
            return false;
        }
        if (!LegacyCodecSupport.areEqual(legacyBegin.getDesiredCapabilities(), newBegin.getDesiredCapabilities())) {
            return false;
        }
        if (!LegacyCodecSupport.areEqual(legacyBegin.getOfferedCapabilities(), newBegin.getOfferedCapabilities())) {
            return false;
        }

        return true;
    }

    /**
     * Compare a legacy Begin to another legacy Begin instance.
     *
     * @param legacyType1
     *      Array of {@link Attach} instances or null
     * @param legacyType2
     *      Array of {@link Attach} instances or null.
     *
     * @return true if the legacy types are equal.
     */
    public static boolean areEqual(Attach legacyType1, Attach legacyType2) {
        if (legacyType1 == legacyType2) {
            return true;
        } else if (legacyType1 == null || legacyType2 == null) {
            return false;
        }

        if (legacyType1.getHandle() == null) {
            if (legacyType2.getHandle() != null) {
                return false;
            }
        } else if (!legacyType1.getHandle().equals(legacyType2.getHandle())) {
            return false;
        }
        if (legacyType1.getInitialDeliveryCount() == null) {
            if (legacyType2.getInitialDeliveryCount() != null) {
                return false;
            }
        } else if (!legacyType1.getInitialDeliveryCount().equals(legacyType2.getInitialDeliveryCount())) {
            return false;
        }
        if (legacyType1.getMaxMessageSize() == null) {
            if (legacyType2.getMaxMessageSize() != null) {
                return false;
            }
        } else if (!legacyType1.getMaxMessageSize().equals(legacyType2.getMaxMessageSize())) {
            return false;
        }
        if (legacyType1.getName() == null) {
            if (legacyType2.getName() != null) {
                return false;
            }
        } else if (!legacyType1.getName().equals(legacyType2.getName())) {
            return false;
        }
        if (legacyType1.getSource() == null) {
            if (legacyType2.getSource() != null) {
                return false;
            }
        } else if (!legacyType1.getSource().equals(legacyType2.getSource())) {
            return false;
        }
        if (legacyType1.getTarget() == null) {
            if (legacyType2.getTarget() != null) {
                return false;
            }
        } else if (!legacyType1.getTarget().equals(legacyType2.getTarget())) {
            return false;
        }
        if (legacyType1.getUnsettled() == null) {
            if (legacyType2.getUnsettled() != null) {
                return false;
            }
        } else if (!legacyType1.getUnsettled().equals(legacyType2.getUnsettled())) {
            return false;
        }
        if (legacyType1.getRcvSettleMode() != legacyType2.getRcvSettleMode()) {
            return false;
        }
        if (legacyType1.getSndSettleMode() != legacyType2.getSndSettleMode()) {
            return false;
        }
        if (legacyType1.getRole() != legacyType2.getRole()) {
            return false;
        }
        if (legacyType1.getIncompleteUnsettled() != legacyType2.getIncompleteUnsettled()) {
            return false;
        }
        if (legacyType1.getProperties() == null) {
            if (legacyType2.getProperties() != null) {
                return false;
            }
        } else if (!legacyType1.getProperties().equals(legacyType2.getProperties())) {
            return false;
        }
        if (!Arrays.equals(legacyType1.getDesiredCapabilities(), legacyType2.getDesiredCapabilities())) {
            return false;
        }
        if (!Arrays.equals(legacyType1.getOfferedCapabilities(), legacyType2.getOfferedCapabilities())) {
            return false;
        }

        return true;
    }

    /**
     * Compare a legacy Attach to an Attach instance using the proton4j Attach type
     *
     * @param newAttach
     *      Array of {@link org.apache.qpid.proton4j.amqp.transport.Attach} instances or null
     * @param legacyAttach
     *      Array of {@link Attach} instances or null.
     *
     * @return true if the legacy and new types are equal.
     */
    public static boolean areEqual(org.apache.qpid.proton4j.amqp.transport.Attach newAttach, Attach legacyAttach) {
        return areEqual(legacyAttach, newAttach);
    }

    /**
     * Compare a legacy Attach to an Attach instance using the proton4j Attach type
     *
     * @param legacyAttach
     *      Array of {@link Attach} instances or null.
     * @param newAttach
     *      Array of {@link org.apache.qpid.proton4j.amqp.transport.Attach} instances or null
     *
     * @return true if the legacy and new types are equal.
     */
    @SuppressWarnings("unchecked")
    public static boolean areEqual(Attach legacyAttach, org.apache.qpid.proton4j.amqp.transport.Attach newAttach) {
        if (legacyAttach == null && newAttach == null) {
            return true;
        } else if (legacyAttach == null || newAttach == null) {
            return false;
        }

        if (legacyAttach.getHandle() == null) {
            if (newAttach.hasHandle()) {
                return false;
            }
        } else if (legacyAttach.getHandle().intValue() != newAttach.getHandle()) {
            return false;
        }
        if (legacyAttach.getInitialDeliveryCount() == null) {
            if (newAttach.hasInitialDeliveryCount()) {
                return false;
            }
        } else if (legacyAttach.getInitialDeliveryCount().intValue() != newAttach.getInitialDeliveryCount()) {
            return false;
        }
        if (legacyAttach.getMaxMessageSize() == null) {
            if (newAttach.getMaxMessageSize() != null) {
                return false;
            }
        } else if (legacyAttach.getMaxMessageSize().longValue() != newAttach.getMaxMessageSize().longValue()) {
            return false;
        }
        if (legacyAttach.getName() == null) {
            if (newAttach.getName() != null) {
                return false;
            }
        } else if (!legacyAttach.getName().equals(newAttach.getName())) {
            return false;
        }
        if (legacyAttach.getSource() == null) {
            if (newAttach.getSource() != null) {
                return false;
            }
        } else if (!legacyAttach.getSource().equals(newAttach.getSource())) {  // TODO
            return false;
        }
        if (legacyAttach.getTarget() == null) {
            if (newAttach.getTarget() != null) {
                return false;
            }
        } else if (!legacyAttach.getTarget().equals(newAttach.getTarget())) {  // TODO
            return false;
        }

        if (legacyAttach.getRcvSettleMode().ordinal() != newAttach.getRcvSettleMode().ordinal()) {
            return false;
        }
        if (legacyAttach.getSndSettleMode().ordinal() != newAttach.getSndSettleMode().ordinal()) {
            return false;
        }
        if (legacyAttach.getRole().ordinal() != newAttach.getRole().ordinal()) {
            return false;
        }
        if (legacyAttach.getIncompleteUnsettled() != newAttach.getIncompleteUnsettled()) {
            return false;
        }
        if (!areUnsettledMapsEqual(legacyAttach.getUnsettled(), newAttach.getUnsettled())) {
            return false;
        }
        if (!areSymbolKeyMapsEqual(legacyAttach.getProperties(), newAttach.getProperties())) {
            return false;
        }
        if (!areEqual(legacyAttach.getDesiredCapabilities(), newAttach.getDesiredCapabilities())) {
            return false;
        }
        if (!areEqual(legacyAttach.getOfferedCapabilities(), newAttach.getOfferedCapabilities())) {
            return false;
        }

        return true;
    }

    /**
     * Compare a legacy Properties Map to an new Properties Map using the proton4j Symbol type keys
     *
     * @param legacy
     *      Array of {@link Map} instances or null.
     * @param properties
     *      Array of {@link Map} instances or null
     *
     * @return true if the arrays contain matching underlying Map values in the same order.
     */
    public static boolean areSymbolKeyMapsEqual(Map<Symbol, Object> legacy, Map<org.apache.qpid.proton4j.amqp.Symbol, Object> properties) {
        if (legacy == null && properties == null) {
            return true;
        } else if (legacy == null || properties == null) {
            return false;
        }

        if (properties.size() != legacy.size()) {
            return false;
        }

        Iterator<Entry<Symbol, Object>> legacyEntries = legacy.entrySet().iterator();
        Iterator<Entry<org.apache.qpid.proton4j.amqp.Symbol, Object>> propertiesEntries = properties.entrySet().iterator();

        while (legacyEntries.hasNext()) {
            Entry<Symbol, Object> legacyEntry = legacyEntries.next();
            Entry<org.apache.qpid.proton4j.amqp.Symbol, Object> propertyEntry = propertiesEntries.next();

            if (!legacyEntry.getKey().toString().equals(propertyEntry.getKey().toString())) {
                return false;
            }
            if (!legacyEntry.getValue().equals(propertyEntry.getValue())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Compare a legacy Unsettled Map to an new Unsettled Map using the proton4j Binary type keys
     * and DeliveryState values
     *
     * @param legacy
     *      Array of {@link Map} instances or null.
     * @param unsettled
     *      Array of {@link Map} instances or null
     *
     * @return true if the arrays contain matching underlying Map values in the same order.
     */
    public static boolean areUnsettledMapsEqual(Map<Binary, DeliveryState> legacy, Map<org.apache.qpid.proton4j.amqp.Binary, org.apache.qpid.proton4j.amqp.transport.DeliveryState> unsettled) {
        if (legacy == null && unsettled == null) {
            return true;
        } else if (legacy == null || unsettled == null) {
            return false;
        }

        if (unsettled.size() != legacy.size()) {
            return false;
        }

        Iterator<Entry<Binary, DeliveryState>> legacyEntries = legacy.entrySet().iterator();
        Iterator<Entry<org.apache.qpid.proton4j.amqp.Binary, org.apache.qpid.proton4j.amqp.transport.DeliveryState>> unsettledEntries = unsettled.entrySet().iterator();

        while (legacyEntries.hasNext()) {
            Entry<Binary, DeliveryState> legacyEntry = legacyEntries.next();
            Entry<org.apache.qpid.proton4j.amqp.Binary, org.apache.qpid.proton4j.amqp.transport.DeliveryState> unsettledEntry = unsettledEntries.next();

            ByteBuffer legacyBuffer = legacyEntry.getKey().asByteBuffer();
            ByteBuffer unsettledBuffer = unsettledEntry.getKey().asByteBuffer();

            if (!legacyBuffer.equals(unsettledBuffer)) {
                return false;
            }
            if (legacyEntry.getValue().getType().ordinal() != unsettledEntry.getValue().getType().ordinal()) {
                return false;
            }
        }

        return true;
    }

    /**
     * Compare a legacy Symbol Array to an Symbol array using the proton4j Symbol type
     *
     * @param newSymbols
     *      Array of {@link org.apache.qpid.proton4j.amqp.Symbol} instances or null
     * @param legacySymbols
     *      Array of {@link Symbol} instances or null.
     *
     * @return true if the arrays contain matching underlying Symbol values in the same order.
     */
    public static boolean areEqual(org.apache.qpid.proton4j.amqp.Symbol[] newSymbols, Symbol[] legacySymbols) {
        return areEqual(legacySymbols, newSymbols);
    }

    /**
     * Compare a legacy Symbol Array to an Symbol array using the proton4j Symbol type
     *
     * @param legacySymbols
     *      Array of {@link Symbol} instances or null.
     * @param newSymbols
     *      Array of {@link org.apache.qpid.proton4j.amqp.Symbol} instances or null
     *
     * @return true if the arrays contain matching underlying Symbol values in the same order.
     */
    public static boolean areEqual(Symbol[] legacySymbols, org.apache.qpid.proton4j.amqp.Symbol[] newSymbols) {
        if (legacySymbols == null && newSymbols == null) {
            return true;
        } else if (legacySymbols == null || newSymbols == null) {
            return false;
        }

        int length = legacySymbols.length;
        if (newSymbols.length != length) {
            return false;
        }

        for (int i=0; i<length; i++) {
            Object symbolOld = legacySymbols[i];
            Object symbolNew = newSymbols[i];
            if (!(symbolOld == null ? symbolNew == null : symbolOld.toString().equals(symbolNew.toString()))) {
                return false;
            }
        }

        return true;
    }
}
