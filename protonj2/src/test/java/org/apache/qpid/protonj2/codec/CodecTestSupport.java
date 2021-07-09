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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.qpid.protonj2.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamDecoderFactory;
import org.apache.qpid.protonj2.codec.encoders.ProtonEncoderFactory;
import org.apache.qpid.protonj2.codec.legacy.LegacyCodecAdapter;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.messaging.Terminus;
import org.apache.qpid.protonj2.types.transactions.Coordinator;
import org.apache.qpid.protonj2.types.transport.Attach;
import org.apache.qpid.protonj2.types.transport.Begin;
import org.apache.qpid.protonj2.types.transport.Close;
import org.apache.qpid.protonj2.types.transport.DeliveryState;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.apache.qpid.protonj2.types.transport.Open;
import org.junit.jupiter.api.BeforeEach;

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

    protected StreamDecoderState streamDecoderState;
    protected StreamDecoder streamDecoder;

    protected Random random = new Random();
    protected long currentSeed;

    protected final LegacyCodecAdapter legacyCodec = new LegacyCodecAdapter();

    @BeforeEach
    public void setUp() {
        decoder = ProtonDecoderFactory.create();
        decoderState = decoder.newDecoderState();

        encoder = ProtonEncoderFactory.create();
        encoderState = encoder.newEncoderState();

        streamDecoder = ProtonStreamDecoderFactory.create();
        streamDecoderState = streamDecoder.newDecoderState();

        currentSeed = System.nanoTime();
        random.setSeed(currentSeed);
    }

    /**
     * Compare a Open to another Open instance.
     *
     * @param open1
     *      An {@link Open} instances or null
     * @param open2
     *      An {@link Open} instances or null.
     *
     * @throws AssertionError
     *      If the two types are not equal to one another.
     */
    public static void assertTypesEqual(Open open1, Open open2) throws AssertionError {
        if (open1 == null && open2 == null) {
            return;
        } else if (open1 == null || open2 == null) {
            assertEquals(open1, open2);
        }

        assertEquals(open1.getChannelMax(), open2.getChannelMax(), "Channel max values not equal");
        assertEquals(open1.getContainerId(), open2.getContainerId(), "Container Id values not equal");
        assertEquals(open1.getHostname(), open2.getHostname(), "Hostname values not equal");
        assertEquals(open1.getIdleTimeout(), open2.getIdleTimeout(), "Idle timeout values not equal");
        assertEquals(open1.getMaxFrameSize(), open2.getMaxFrameSize(), "Max Frame Size values not equal");
        assertEquals(open1.getProperties(), open2.getProperties(), "Properties Map values not equal");
        assertArrayEquals(open1.getDesiredCapabilities(), open2.getDesiredCapabilities(), "Desired Capabilities are not equal");
        assertArrayEquals(open1.getOfferedCapabilities(), open2.getOfferedCapabilities(), "Offered Capabilities are not equal");
        assertArrayEquals(open1.getIncomingLocales(), open2.getIncomingLocales(), "Incoming Locales are not equal");
        assertArrayEquals(open1.getOutgoingLocales(), open2.getOutgoingLocales(), "Outgoing Locales are not equal");
    }

    /**
     * Compare a Close to another Close instance.
     *
     * @param close1
     *      An {@link Close} instances or null
     * @param close2
     *      An {@link Close} instances or null.
     *
     * @throws AssertionError
     *      If the two types are not equal to one another.
     */
    public static void assertTypesEqual(Close close1, Close close2) throws AssertionError {
        if (close1 == null && close2 == null) {
            return;
        } else if (close1 == null || close2 == null) {
            assertEquals(close1, close2);
        }

        assertTypesEqual(close1.getError(), close2.getError());
    }

    /**
     * Compare a Begin to another Begin instance.
     *
     * @param begin1
     *      A {@link Begin} instances or null
     * @param begin2
     *      A {@link Begin} instances or null.
     *
     * @throws AssertionError
     *      If the two types are not equal to one another.
     */
    public static void assertTypesEqual(Begin begin1, Begin begin2) throws AssertionError {
        if (begin1 == null && begin2 == null) {
            return;
        } else if (begin1 == null || begin2 == null) {
            assertEquals(begin1, begin2);
        }

        assertSame(begin1.hasHandleMax(), begin2.hasHandleMax(), "Expected Begin with matching has handle max values");
        assertEquals(begin1.getHandleMax(), begin2.getHandleMax(), "Handle max values not equal");

        assertSame(begin1.hasIncomingWindow(), begin2.hasIncomingWindow(), "Expected Begin with matching has Incoming window values");
        assertEquals(begin1.getIncomingWindow(), begin2.getIncomingWindow(), "Incoming Window values not equal");

        assertSame(begin1.hasNextOutgoingId(), begin2.hasNextOutgoingId(), "Expected Begin with matching has Outgoing Id values");
        assertEquals(begin1.getNextOutgoingId(), begin2.getNextOutgoingId(), "Outgoing Id values not equal");

        assertSame(begin1.hasOutgoingWindow(), begin2.hasOutgoingWindow(), "Expected Begin with matching has Outgoing window values");
        assertEquals(begin1.getOutgoingWindow(), begin2.getOutgoingWindow(), "Outgoing Window values not equal");

        assertSame(begin1.hasRemoteChannel(), begin2.hasRemoteChannel(), "Expected Begin with matching has Remote Channel values");
        assertEquals(begin1.getRemoteChannel(), begin2.getRemoteChannel(), "Remote Channel values not equal");

        assertSame(begin1.hasProperties(), begin2.hasProperties(), "Expected Attach with matching has properties values");
        assertEquals(begin1.getProperties(), begin2.getProperties(), "Properties Map values not equal");
        assertSame(begin1.hasDesiredCapabilities(), begin2.hasDesiredCapabilities(), "Expected Attach with matching has desired capabilities values");
        assertArrayEquals(begin1.getDesiredCapabilities(), begin2.getDesiredCapabilities(), "Desired Capabilities are not equal");
        assertSame(begin2.hasOfferedCapabilities(), begin2.hasOfferedCapabilities(), "Expected Attach with matching has offered capabilities values");
        assertArrayEquals(begin1.getOfferedCapabilities(), begin2.getOfferedCapabilities(), "Offered Capabilities are not equal");
    }

    /**
     * Compare a Attach to another Attach instance.
     *
     * @param attach1
     *      A {@link Attach} instances or null
     * @param attach2
     *      A {@link Attach} instances or null.
     *
     * @throws AssertionError
     *      If the two types are not equal to one another.
     */
    public static void assertTypesEqual(Attach attach1, Attach attach2) throws AssertionError {
        if (attach1 == attach2) {
            return;
        } else if (attach1 == null || attach2 == null) {
            assertEquals(attach1, attach2);
        }

        assertSame(attach1.hasHandle(), attach2.hasHandle(), "Expected Attach with matching has handle values");
        assertEquals(attach1.getHandle(), attach2.getHandle(), "Handle values not equal");

        assertSame(attach1.hasInitialDeliveryCount(), attach2.hasInitialDeliveryCount(), "Expected Attach with matching has initial delivery count values");
        assertEquals(attach1.getInitialDeliveryCount(), attach2.getInitialDeliveryCount(), "Initial delivery count values not equal");

        assertSame(attach1.hasMaxMessageSize(), attach2.hasMaxMessageSize(), "Expected Attach with matching has max message size values");
        assertEquals(attach1.getMaxMessageSize(), attach2.getMaxMessageSize(), "Max MessageSize values not equal");

        assertSame(attach1.hasName(), attach2.hasName(), "Expected Attach with matching has name values");
        assertEquals(attach1.getName(), attach2.getName(), "Link Name values not equal");

        assertSame(attach1.hasSource(), attach2.hasSource(), "Expected Attach with matching has Source values");
        assertTypesEqual(attach1.getSource(), attach2.getSource());
        assertSame(attach1.hasTarget(), attach2.hasTarget(), "Expected Attach with matching has Target values");
        Target attach1Target = attach1.getTarget();
        Target attach2Target = attach2.getTarget();
        assertTypesEqual(attach1Target, attach2Target);

        assertSame(attach1.hasUnsettled(), attach2.hasUnsettled(), "Expected Attach with matching has handle values");
        assertTypesEqual(attach1.getUnsettled(), attach2.getUnsettled());

        assertSame(attach1.hasReceiverSettleMode(), attach2.hasReceiverSettleMode(), "Expected Attach with matching has receiver settle mode values");
        assertEquals(attach1.getReceiverSettleMode(), attach2.getReceiverSettleMode(), "Receiver settle mode values not equal");

        assertSame(attach1.hasSenderSettleMode(), attach2.hasSenderSettleMode(), "Expected Attach with matching has sender settle mode values");
        assertEquals(attach1.getSenderSettleMode(), attach2.getSenderSettleMode(), "Sender settle mode values not equal");

        assertSame(attach1.hasRole(), attach2.hasRole(), "Expected Attach with matching has Role values");
        assertEquals(attach1.getRole(), attach2.getRole(), "Role values not equal");

        assertSame(attach1.hasIncompleteUnsettled(), attach2.hasIncompleteUnsettled(), "Expected Attach with matching has incomplete unsettled values");
        assertEquals(attach1.getIncompleteUnsettled(), attach2.getIncompleteUnsettled(), "Handle values not equal");

        assertSame(attach1.hasProperties(), attach2.hasProperties(), "Expected Attach with matching has properties values");
        assertEquals(attach1.getProperties(), attach2.getProperties(), "Properties Map values not equal");
        assertSame(attach1.hasDesiredCapabilities(), attach2.hasDesiredCapabilities(), "Expected Attach with matching has desired capabilities values");
        assertArrayEquals(attach1.getDesiredCapabilities(), attach2.getDesiredCapabilities(), "Desired Capabilities are not equal");
        assertSame(attach1.hasOfferedCapabilities(), attach2.hasOfferedCapabilities(), "Expected Attach with matching has offered capabilities values");
        assertArrayEquals(attach1.getOfferedCapabilities(), attach2.getOfferedCapabilities(), "Offered Capabilities are not equal");
    }

    /**
     * Compare a Target to another Target instance.
     *
     * @param terminus1
     *      A {@link Terminus} instances or null
     * @param terminus2
     *      A {@link Terminus} instances or null.
     *
     * @throws AssertionError
     *      If the two types are not equal to one another.
     */
    public static void assertTypesEqual(Terminus terminus1, Terminus terminus2) throws AssertionError {
        if (terminus1 == terminus2) {
            return;
        } else if (terminus1 == null || terminus2 == null) {
            assertEquals(terminus1, terminus2);
        } else if (terminus1.getClass().equals(terminus2.getClass())) {
            fail("Terminus types are not equal");
        }

        if (terminus1 instanceof Source) {
            assertTypesEqual((Source) terminus1, (Source) terminus2);
        } else if (terminus1 instanceof Target) {
            assertTypesEqual((Target) terminus1, (Target) terminus2);
        } else if (terminus1 instanceof Coordinator) {
            assertTypesEqual(terminus1, terminus2);
        } else {
            fail("Terminus types are of unknown origin.");
        }
    }

    /**
     * Compare a Target to another Target instance.
     *
     * @param target1
     *      A {@link Target} instances or null
     * @param target2
     *      A {@link Target} instances or null.
     *
     * @throws AssertionError
     *      If the two types are not equal to one another.
     */
    public static void assertTypesEqual(Target target1, Target target2) throws AssertionError {
        if (target1 == target2) {
            return;
        } else if (target1 == null || target2 == null) {
            assertEquals(target1, target2);
        }

        assertEquals(target1.getAddress(), target2.getAddress(), "Addrress values not equal");
        assertEquals(target1.getDurable(), target2.getDurable(), "TerminusDurability values not equal");
        assertEquals(target1.getExpiryPolicy(), target2.getExpiryPolicy(), "TerminusExpiryPolicy values not equal");
        assertEquals(target1.getTimeout(), target2.getTimeout(), "Timeout values not equal");
        assertEquals(target1.isDynamic(), target2.isDynamic(), "Dynamic values not equal");
        assertEquals(target1.getDynamicNodeProperties(), target2.getDynamicNodeProperties(), "Dynamic Node Properties values not equal");
        assertArrayEquals(target1.getCapabilities(), target2.getCapabilities(), "Capabilities values not equal");
    }

    /**
     * Compare a Target to another Target instance.
     *
     * @param coordinator1
     *      A {@link Coordinator} instances or null
     * @param coordinator2
     *      A {@link Coordinator} instances or null.
     *
     * @throws AssertionError
     *      If the two types are not equal to one another.
     */
    public static void assertTypesEqual(Coordinator coordinator1, Coordinator coordinator2) throws AssertionError {
        if (coordinator1 == coordinator2) {
            return;
        } else if (coordinator1 == null || coordinator2 == null) {
            assertEquals(coordinator1, coordinator2);
        }

        assertArrayEquals(coordinator1.getCapabilities(), coordinator2.getCapabilities(), "Capabilities values not equal");
    }

    /**
     * Compare a Source to another Source instance.
     *
     * @param source1
     *      A {@link Source} instances or null
     * @param source2
     *      A {@link Source} instances or null.
     *
     * @throws AssertionError
     *      If the two types are not equal to one another.
     */
    public static void assertTypesEqual(Source source1, Source source2) throws AssertionError {
        if (source1 == source2) {
            return;
        } else if (source1 == null || source2 == null) {
            assertEquals(source1, source2);
        }

        assertEquals(source1.getAddress(), source2.getAddress(), "Addrress values not equal");
        assertEquals(source1.getDurable(), source2.getDurable(), "TerminusDurability values not equal");
        assertEquals(source1.getExpiryPolicy(), source2.getExpiryPolicy(), "TerminusExpiryPolicy values not equal");
        assertEquals(source1.getTimeout(), source2.getTimeout(), "Timeout values not equal");
        assertEquals(source1.isDynamic(), source2.isDynamic(), "Dynamic values not equal");
        assertEquals(source1.getDynamicNodeProperties(), source2.getDynamicNodeProperties(), "Dynamic Node Properties values not equal");
        assertEquals(source1.getDistributionMode(), source2.getDistributionMode(), "Distribution Mode values not equal");
        assertEquals(source1.getDefaultOutcome(), source2.getDefaultOutcome(), "Filter values not equal");
        assertEquals(source1.getFilter(), source2.getFilter(), "Default outcome values not equal");
        assertArrayEquals(source1.getOutcomes(), source2.getOutcomes(), "Outcomes values not equal");
        assertArrayEquals(source1.getCapabilities(), source2.getCapabilities(), "Capabilities values not equal");
    }

    /**
     * Compare a ErrorCondition to another ErrorCondition instance.
     *
     * @param condition1
     *      A {@link ErrorCondition} instances or null
     * @param condition2
     *      A {@link ErrorCondition} instances or null.
     *
     * @throws AssertionError
     *      If the two types are not equal to one another.
     */
    public static void assertTypesEqual(ErrorCondition condition1, ErrorCondition condition2) throws AssertionError {
        if (condition1 == condition2) {
            return;
        } else if (condition1 == null || condition2 == null) {
            assertEquals(condition1, condition2);
        }

        assertEquals(condition1.getDescription(), condition2.getDescription(), "Error Descriptions should match");
        assertEquals(condition1.getCondition(), condition2.getCondition(), "Error Condition should match");
        assertEquals(condition1.getInfo(), condition2.getInfo(), "Error Info should match");
    }

    /**
     * Compare a Unsettled Map to another Unsettled Map using the proton Binary type keys
     * and DeliveryState values
     *
     * @param unsettled1
     *      A {@link Map} instances or null.
     * @param unsettled2
     *      A {@link Map} instances or null
     *
     * @throws AssertionError
     *      If the two types are not equal to one another.
     */
    public static void assertTypesEqual(Map<Binary, DeliveryState> unsettled1, Map<Binary, DeliveryState> unsettled2) throws AssertionError {
        if (unsettled1 == null && unsettled2 == null) {
            return;
        } else if (unsettled1 == null || unsettled2 == null) {
            assertEquals(unsettled1, unsettled2);
        }

        assertEquals(unsettled1.size(), unsettled2.size(), "Unsettled Map size values are not the same");

        Iterator<Entry<Binary, DeliveryState>> legacyEntries = unsettled1.entrySet().iterator();
        Iterator<Entry<Binary, DeliveryState>> unsettledEntries = unsettled2.entrySet().iterator();

        while (legacyEntries.hasNext()) {
            Entry<Binary, DeliveryState> legacyEntry = legacyEntries.next();
            Entry<Binary, DeliveryState> unsettledEntry = unsettledEntries.next();

            ByteBuffer legacyBuffer = legacyEntry.getKey().asByteBuffer();
            ByteBuffer unsettledBuffer = unsettledEntry.getKey().asByteBuffer();

            assertEquals(legacyBuffer, unsettledBuffer, "Delivery Tags do not match");
            assertEquals(legacyEntry.getValue().getType(), unsettledEntry.getValue().getType(), "Delivery States do not match");
        }
    }
}
