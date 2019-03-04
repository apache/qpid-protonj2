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
package org.apache.qpid.proton4j.codec;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.proton4j.codec.encoders.ProtonEncoderFactory;
import org.apache.qpid.proton4j.codec.legacy.LegacyCodecAdapter;
import org.junit.Before;

/**
 * Support class for tests of the type decoders
 */
public class CodecTestSupport {

    protected static final int LARGE_SIZE = 1024;
    protected static final int SMALL_SIZE = 32;

    protected static final int LARGE_ARRAY_SIZE = 1024;
    protected static final int SMALL_ARRAY_SIZE = 32;

    protected DecoderState decoderState;
    protected EncoderState encoderState;
    protected Decoder decoder;
    protected Encoder encoder;

    protected final LegacyCodecAdapter legacyCodec = new LegacyCodecAdapter();

    @Before
    public void setUp() {
        decoder = ProtonDecoderFactory.create();
        decoderState = decoder.newDecoderState();

        encoder = ProtonEncoderFactory.create();
        encoderState = encoder.newEncoderState();
    }

    // TODO Create proper assert methods that assert with reasonable messages.

    /**
     * Compare a Open to another Open instance.
     *
     * @param open1
     *      An {@link Open} instances or null
     * @param open2
     *      An {@link Open} instances or null.
     *
     * @return true if the types are equal.
     */
    public static boolean areEqual(Open open1, Open open2) {
        if (open1 == null && open2 == null) {
            return true;
        } else if (open1 == null || open2 == null) {
            return false;
        }

        if (open1.getChannelMax() == null) {
            if (open2.getChannelMax() != null) {
                return false;
            }
        } else if (!open1.getChannelMax().equals(open2.getChannelMax())) {
            return false;
        }

        if (open1.getContainerId() == null) {
            if (open2.getContainerId() != null) {
                return false;
            }
        } else if (!open1.getContainerId().equals(open2.getContainerId())) {
            return false;
        }

        if (open1.getHostname() == null) {
            if (open2.getHostname() != null) {
                return false;
            }
        } else if (!open1.getHostname().equals(open2.getHostname())) {
            return false;
        }

        if (open1.getIdleTimeOut() == null) {
            if (open2.getIdleTimeOut() != null) {
                return false;
            }
        } else if (!open1.getIdleTimeOut().equals(open2.getIdleTimeOut())) {
            return false;
        }

        if (open1.getMaxFrameSize() == null) {
            if (open2.getMaxFrameSize() != null) {
                return false;
            }
        } else if (!open1.getMaxFrameSize().equals(open2.getMaxFrameSize())) {
            return false;
        }

        if (open1.getProperties() == null) {
            if (open2.getProperties() != null) {
                return false;
            }
        } else if (!open1.getProperties().equals(open2.getProperties())) {
            return false;
        }

        if (!Arrays.equals(open1.getDesiredCapabilities(), open2.getDesiredCapabilities())) {
            return false;
        }
        if (!Arrays.equals(open1.getOfferedCapabilities(), open2.getOfferedCapabilities())) {
            return false;
        }
        if (!Arrays.equals(open1.getIncomingLocales(), open2.getIncomingLocales())) {
            return false;
        }
        if (!Arrays.equals(open1.getOutgoingLocales(), open2.getOutgoingLocales())) {
            return false;
        }

        return true;
    }

    /**
     * Compare a Begin to another Begin instance.
     *
     * @param begin1
     *      A {@link Begin} instances or null
     * @param begin2
     *      A {@link Begin} instances or null.
     *
     * @return true if the types are equal.
     */
    public static boolean areEqual(Begin begin1, Begin begin2) {
        if (begin1 == null && begin2 == null) {
            return true;
        } else if (begin1 == null || begin2 == null) {
            return false;
        }

        if (!begin1.hasHandleMax() && begin2.hasHandleMax() ||
            !begin2.hasHandleMax() && begin1.hasHandleMax()) {
            return false;
        } else if (begin1.getHandleMax() != begin2.getHandleMax()) {
            return false;
        }
        if (!begin1.hasIncomingWindow() && begin2.hasIncomingWindow() ||
            !begin2.hasIncomingWindow() && begin1.hasIncomingWindow()) {
            return false;
        } else if (begin1.getIncomingWindow() != begin2.getIncomingWindow()) {
            return false;
        }
        if (!begin1.hasNextOutgoingId() && begin2.hasNextOutgoingId() ||
            !begin2.hasNextOutgoingId() && begin1.hasNextOutgoingId()) {
            return false;
        } else if (begin1.getNextOutgoingId() != begin2.getNextOutgoingId()) {
            return false;
        }
        if (!begin1.hasOutgoingWindow() && begin2.hasOutgoingWindow() ||
            !begin2.hasOutgoingWindow() && begin1.hasOutgoingWindow()) {
            return false;
        } else if (begin1.getOutgoingWindow() != begin2.getOutgoingWindow()) {
            return false;
        }
        if (!begin1.hasRemoteChannel() && begin2.hasRemoteChannel() ||
            !begin2.hasRemoteChannel() && begin1.hasRemoteChannel()) {
            return false;
        } else if (begin1.getRemoteChannel() != begin2.getRemoteChannel()) {
            return false;
        }
        if (!begin1.hasProperties() && begin2.hasProperties() ||
            !begin2.hasProperties() && begin1.hasProperties()) {
            return false;
        } if (begin1.hasProperties() && !begin1.getProperties().equals(begin2.getProperties())) {
            return false;
        }
        if (!Arrays.equals(begin1.getDesiredCapabilities(), begin2.getDesiredCapabilities())) {
            return false;
        }
        if (!Arrays.equals(begin1.getOfferedCapabilities(), begin2.getOfferedCapabilities())) {
            return false;
        }

        return true;
    }

    /**
     * Compare a Attach to another Attach instance.
     *
     * @param attach1
     *      A {@link Attach} instances or null
     * @param attach2
     *      A {@link Attach} instances or null.
     *
     * @return true if the types are equal.
     */
    public static boolean areEqual(Attach attach1, Attach attach2) {
        if (attach1 == attach2) {
            return true;
        } else if (attach1 == null || attach2 == null) {
            return false;
        }

        if (!attach1.hasHandle() && attach2.hasHandle() ||
            !attach2.hasHandle() && attach1.hasHandle()) {
            return false;
        } else if (attach1.getHandle() != attach2.getHandle()) {
            return false;
        }
        if (!attach1.hasInitialDeliveryCount() && attach2.hasInitialDeliveryCount() ||
            !attach2.hasInitialDeliveryCount() && attach1.hasInitialDeliveryCount()) {
            return false;
        } else if (attach1.getInitialDeliveryCount() != attach2.getInitialDeliveryCount()) {
            return false;
        }
        if (!attach1.hasMaxMessageSize() && attach2.hasMaxMessageSize() ||
            !attach2.hasMaxMessageSize() && attach1.hasMaxMessageSize()) {
            return false;
        } else if (attach1.getMaxMessageSize() != attach2.getMaxMessageSize()) {
            return false;
        }
        if (!attach1.hasName() && attach2.hasName() ||
            !attach2.hasName() && attach1.hasName()) {
            return false;
        } else if (!attach1.getName().equals(attach2.getName())) {
            return false;
        }
        if (!attach1.hasSource() && attach2.hasSource() ||
            !attach2.hasSource() && attach1.hasSource()) {
            return false;
        } else if (!attach1.getSource().equals(attach2.getSource())) {
            return false;
        }
        if (!attach1.hasTarget() && attach2.hasTarget() ||
            !attach2.hasTarget() && attach1.hasTarget()) {
            return false;
        } else if (!attach1.getTarget().equals(attach2.getTarget())) {
            return false;
        }
        if (!attach1.hasUnsettled() && attach2.hasUnsettled() ||
            !attach2.hasUnsettled() && attach1.hasUnsettled()) {
            return false;
        } else if (areUnsettledMapsEqual(attach1.getUnsettled(), attach2.getUnsettled())) {
            return false;
        }
        if (attach1.getRcvSettleMode() != attach2.getRcvSettleMode()) {
            return false;
        }
        if (attach1.getSndSettleMode() != attach2.getSndSettleMode()) {
            return false;
        }
        if (attach1.getRole() != attach2.getRole()) {
            return false;
        }
        if (attach1.getIncompleteUnsettled() != attach2.getIncompleteUnsettled()) {
            return false;
        }
        if (!attach1.hasProperties() && attach2.hasProperties() ||
            !attach2.hasProperties() && attach1.hasProperties()) {
            return false;
        } else if (attach1.getProperties() != null && !attach1.getProperties().equals(attach2.getProperties())) {
            return false;
        }
        if (!Arrays.equals(attach1.getDesiredCapabilities(), attach2.getDesiredCapabilities())) {
            return false;
        }
        if (!Arrays.equals(attach1.getOfferedCapabilities(), attach2.getOfferedCapabilities())) {
            return false;
        }

        return true;
    }

    /**
     * Compare a Unsettled Map to another Unsettled Map using the proton4j Binary type keys
     * and DeliveryState values
     *
     * @param legacy
     *      A {@link Map} instances or null.
     * @param unsettled
     *      A {@link Map} instances or null
     *
     * @return true if the Map contain matching underlying Map values in the same order.
     */
    public static boolean areUnsettledMapsEqual(Map<Binary, DeliveryState> legacy, Map<Binary, DeliveryState> unsettled) {
        if (legacy == null && unsettled == null) {
            return true;
        } else if (legacy == null || unsettled == null) {
            return false;
        }

        if (unsettled.size() != legacy.size()) {
            return false;
        }

        Iterator<Entry<Binary, DeliveryState>> legacyEntries = legacy.entrySet().iterator();
        Iterator<Entry<Binary, DeliveryState>> unsettledEntries = unsettled.entrySet().iterator();

        while (legacyEntries.hasNext()) {
            Entry<Binary, DeliveryState> legacyEntry = legacyEntries.next();
            Entry<Binary, DeliveryState> unsettledEntry = unsettledEntries.next();

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
}
