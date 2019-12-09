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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Close;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.codec.decoders.ProtonDecoderFactory;
import org.apache.qpid.proton4j.codec.encoders.ProtonEncoderFactory;
import org.apache.qpid.proton4j.codec.legacy.LegacyCodecAdapter;
import org.junit.Assert;
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
            Assert.assertEquals(open1, open2);
        }

        Assert.assertEquals("Channel max values not equal", open1.getChannelMax(), open2.getChannelMax());
        Assert.assertEquals("Container Id values not equal", open1.getContainerId(), open2.getContainerId());
        Assert.assertEquals("Hostname values not equal", open1.getHostname(), open2.getHostname());
        Assert.assertEquals("Idle timeout values not equal", open1.getIdleTimeOut(), open2.getIdleTimeOut());
        Assert.assertEquals("Max Frame Size values not equal", open1.getMaxFrameSize(), open2.getMaxFrameSize());
        Assert.assertEquals("Properties Map values not equal", open1.getProperties(), open2.getProperties());
        Assert.assertArrayEquals("Desired Capabilities are not equal", open1.getDesiredCapabilities(), open2.getDesiredCapabilities());
        Assert.assertArrayEquals("Offered Capabilities are not equal", open1.getOfferedCapabilities(), open2.getOfferedCapabilities());
        Assert.assertArrayEquals("Incoming Locales are not equal", open1.getIncomingLocales(), open2.getIncomingLocales());
        Assert.assertArrayEquals("Outgoing Locales are not equal", open1.getOutgoingLocales(), open2.getOutgoingLocales());
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
            Assert.assertEquals(close1, close2);
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
            Assert.assertEquals(begin1, begin2);
        }

        Assert.assertSame("Expected Begin with matching has handle max values", begin1.hasHandleMax(), begin2.hasHandleMax());
        Assert.assertEquals("Handle max values not equal", begin1.getHandleMax(), begin2.getHandleMax());

        Assert.assertSame("Expected Begin with matching has Incoming window values", begin1.hasIncomingWindow(), begin2.hasIncomingWindow());
        Assert.assertEquals("Incoming Window values not equal", begin1.getIncomingWindow(), begin2.getIncomingWindow());

        Assert.assertSame("Expected Begin with matching has Outgoing Id values", begin1.hasNextOutgoingId(), begin2.hasNextOutgoingId());
        Assert.assertEquals("Outgoing Id values not equal", begin1.getNextOutgoingId(), begin2.getNextOutgoingId());

        Assert.assertSame("Expected Begin with matching has Outgoing window values", begin1.hasOutgoingWindow(), begin2.hasOutgoingWindow());
        Assert.assertEquals("Outgoing Window values not equal", begin1.getOutgoingWindow(), begin2.getOutgoingWindow());

        Assert.assertSame("Expected Begin with matching has Remote Channel values", begin1.hasRemoteChannel(), begin2.hasRemoteChannel());
        Assert.assertEquals("Remote Channel values not equal", begin1.getRemoteChannel(), begin2.getRemoteChannel());

        Assert.assertSame("Expected Attach with matching has properties values", begin1.hasProperties(), begin2.hasProperties());
        Assert.assertEquals("Properties Map values not equal", begin1.getProperties(), begin2.getProperties());
        Assert.assertSame("Expected Attach with matching has desired capabilities values", begin1.hasDesiredCapabilites(), begin2.hasDesiredCapabilites());
        Assert.assertArrayEquals("Desired Capabilities are not equal", begin1.getDesiredCapabilities(), begin2.getDesiredCapabilities());
        Assert.assertSame("Expected Attach with matching has offered capabilities values", begin2.hasOfferedCapabilites(), begin2.hasOfferedCapabilites());
        Assert.assertArrayEquals("Offered Capabilities are not equal", begin1.getOfferedCapabilities(), begin2.getOfferedCapabilities());
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
            Assert.assertEquals(attach1, attach2);
        }

        Assert.assertSame("Expected Attach with matching has handle values", attach1.hasHandle(), attach2.hasHandle());
        Assert.assertEquals("Handle values not equal", attach1.getHandle(), attach2.getHandle());

        Assert.assertSame("Expected Attach with matching has initial delivery count values", attach1.hasInitialDeliveryCount(), attach2.hasInitialDeliveryCount());
        Assert.assertEquals("Initial delivery count values not equal", attach1.getInitialDeliveryCount(), attach2.getInitialDeliveryCount());

        Assert.assertSame("Expected Attach with matching has max message size values", attach1.hasMaxMessageSize(), attach2.hasMaxMessageSize());
        Assert.assertEquals("Max MessageSize values not equal", attach1.getMaxMessageSize(), attach2.getMaxMessageSize());

        Assert.assertSame("Expected Attach with matching has name values", attach1.hasName(), attach2.hasName());
        Assert.assertEquals("Link Name values not equal", attach1.getName(), attach2.getName());

        Assert.assertSame("Expected Attach with matching has Source values", attach1.hasSource(), attach2.hasSource());
        assertTypesEqual(attach1.getSource(), attach2.getSource());
        Assert.assertSame("Expected Attach with matching has Target values", attach1.hasTarget(), attach2.hasTarget());
        assertTypesEqual(attach1.getTarget(), attach2.getTarget());

        Assert.assertSame("Expected Attach with matching has handle values", attach1.hasUnsettled(), attach2.hasUnsettled());
        assertTypesEqual(attach1.getUnsettled(), attach2.getUnsettled());

        Assert.assertSame("Expected Attach with matching has receiver settle mode values", attach1.hasReceiverSettleMode(), attach2.hasReceiverSettleMode());
        Assert.assertEquals("Receiver settle mode values not equal", attach1.getReceiverSettleMode(), attach2.getReceiverSettleMode());

        Assert.assertSame("Expected Attach with matching has sender settle mode values", attach1.hasSenderSettleMode(), attach2.hasSenderSettleMode());
        Assert.assertEquals("Sender settle mode values not equal", attach1.getSenderSettleMode(), attach2.getSenderSettleMode());

        Assert.assertSame("Expected Attach with matching has Role values", attach1.hasRole(), attach2.hasRole());
        Assert.assertEquals("Role values not equal", attach1.getRole(), attach2.getRole());

        Assert.assertSame("Expected Attach with matching has incomplete unsettled values", attach1.hasIncompleteUnsettled(), attach2.hasIncompleteUnsettled());
        Assert.assertEquals("Handle values not equal", attach1.getIncompleteUnsettled(), attach2.getIncompleteUnsettled());

        Assert.assertSame("Expected Attach with matching has properties values", attach1.hasProperties(), attach2.hasProperties());
        Assert.assertEquals("Properties Map values not equal", attach1.getProperties(), attach2.getProperties());
        Assert.assertSame("Expected Attach with matching has desired capabilities values", attach1.hasDesiredCapabilites(), attach2.hasDesiredCapabilites());
        Assert.assertArrayEquals("Desired Capabilities are not equal", attach1.getDesiredCapabilities(), attach2.getDesiredCapabilities());
        Assert.assertSame("Expected Attach with matching has offered capabilities values", attach1.hasOfferedCapabilites(), attach2.hasOfferedCapabilites());
        Assert.assertArrayEquals("Offered Capabilities are not equal", attach1.getOfferedCapabilities(), attach2.getOfferedCapabilities());
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
            Assert.assertEquals(target1, target2);
        }

        Assert.assertEquals("Addrress values not equal", target1.getAddress(), target2.getAddress());
        Assert.assertEquals("TerminusDurability values not equal", target1.getDurable(), target2.getDurable());
        Assert.assertEquals("TerminusExpiryPolicy values not equal", target1.getExpiryPolicy(), target2.getExpiryPolicy());
        Assert.assertEquals("Timeout values not equal", target1.getTimeout(), target2.getTimeout());
        Assert.assertEquals("Dynamic values not equal", target1.getDynamic(), target2.getDynamic());
        Assert.assertEquals("Dynamic Node Properties values not equal", target1.getDynamicNodeProperties(), target2.getDynamicNodeProperties());
        Assert.assertArrayEquals("Capabilities values not equal", target1.getCapabilities(), target2.getCapabilities());
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
            Assert.assertEquals(source1, source2);
        }

        Assert.assertEquals("Addrress values not equal", source1.getAddress(), source2.getAddress());
        Assert.assertEquals("TerminusDurability values not equal", source1.getDurable(), source2.getDurable());
        Assert.assertEquals("TerminusExpiryPolicy values not equal", source1.getExpiryPolicy(), source2.getExpiryPolicy());
        Assert.assertEquals("Timeout values not equal", source1.getTimeout(), source2.getTimeout());
        Assert.assertEquals("Dynamic values not equal", source1.getDynamic(), source2.getDynamic());
        Assert.assertEquals("Dynamic Node Properties values not equal", source1.getDynamicNodeProperties(), source2.getDynamicNodeProperties());
        Assert.assertEquals("Distribution Mode values not equal", source1.getDistributionMode(), source2.getDistributionMode());
        Assert.assertEquals("Filter values not equal", source1.getDefaultOutcome(), source2.getDefaultOutcome());
        Assert.assertEquals("Default outcome values not equal", source1.getFilter(), source2.getFilter());
        Assert.assertArrayEquals("Outcomes values not equal", source1.getOutcomes(), source2.getOutcomes());
        Assert.assertArrayEquals("Capabilities values not equal", source1.getCapabilities(), source2.getCapabilities());
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
            Assert.assertEquals(condition1, condition2);
        }

        Assert.assertEquals("Error Descriptions should match", condition1.getDescription(), condition2.getDescription());
        Assert.assertEquals("Error Condition should match", condition1.getCondition(), condition2.getCondition());
        Assert.assertEquals("Error Info should match", condition1.getInfo(), condition2.getInfo());
    }

    /**
     * Compare a Unsettled Map to another Unsettled Map using the proton4j Binary type keys
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
            Assert.assertEquals(unsettled1, unsettled2);
        }

        Assert.assertEquals("Unsettled Map size values are not the same", unsettled1.size(), unsettled2.size());

        Iterator<Entry<Binary, DeliveryState>> legacyEntries = unsettled1.entrySet().iterator();
        Iterator<Entry<Binary, DeliveryState>> unsettledEntries = unsettled2.entrySet().iterator();

        while (legacyEntries.hasNext()) {
            Entry<Binary, DeliveryState> legacyEntry = legacyEntries.next();
            Entry<Binary, DeliveryState> unsettledEntry = unsettledEntries.next();

            ByteBuffer legacyBuffer = legacyEntry.getKey().asByteBuffer();
            ByteBuffer unsettledBuffer = unsettledEntry.getKey().asByteBuffer();

            Assert.assertEquals("Delivery Tags do not match", legacyBuffer, unsettledBuffer);
            Assert.assertEquals("Delivery States do not match", legacyEntry.getValue().getType(), unsettledEntry.getValue().getType());
        }
    }
}
