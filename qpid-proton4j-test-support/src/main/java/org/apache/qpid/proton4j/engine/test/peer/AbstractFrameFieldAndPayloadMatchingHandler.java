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
package org.apache.qpid.proton4j.engine.test.peer;

import java.util.List;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.DescribedType;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.engine.test.EngineTestDriver;
import org.apache.qpid.proton4j.engine.test.describedtypes.FrameDescriptorMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFrameFieldAndPayloadMatchingHandler extends AbstractFieldAndDescriptorMatcher implements FrameHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFrameFieldAndPayloadMatchingHandler.class);

    public static int ANY_CHANNEL = -1;

    private final FrameType frameType;

    /** The expected channel number, or {@link #ANY_CHANNEL} if we don't care */
    private int expectedChannel;
    private int actualChannel;

    private Runnable onCompletion; // TODO

    private int expectedFrameSize;

    protected AbstractFrameFieldAndPayloadMatchingHandler(FrameType frameType, int channel, int frameSize, UnsignedLong numericDescriptor, Symbol symbolicDescriptor) {
        super(numericDescriptor, symbolicDescriptor);

        this.frameType = frameType;
        this.expectedChannel = channel;
        this.expectedFrameSize = frameSize;
    }

    protected abstract void verifyPayload(Binary payload) throws AssertionError;

    @SuppressWarnings("unchecked")
    @Override
    public final void frame(int type, int ch, int frameSize, DescribedType dt, Binary payload, EngineTestDriver peer) {
        if (type == frameType.ordinal() && (expectedChannel == ANY_CHANNEL || expectedChannel == ch) && descriptorMatches(dt.getDescriptor())
            && (dt.getDescribed() instanceof List)) {
            actualChannel = ch;

            try {
                verifyFields((List<Object>) dt.getDescribed());
            } catch (AssertionError ae) {
                LOGGER.error("Failure when verifying frame fields", ae);
                peer.assertionFailed(ae);
            }

            try {
                verifyPayload(payload);
            } catch (AssertionError ae) {
                LOGGER.error("Failure when verifying frame payload", ae);
                peer.assertionFailed(ae);
            }

            if (expectedFrameSize != 0 && expectedFrameSize != frameSize) {
                throw new IllegalArgumentException(String.format(
                    "Frame size was not as expected. Expected: " + "type=%s, channel=%s, size=%s, descriptor=%s/%s but got: " + "type=%s, channel=%s, size=%s",
                    frameType.ordinal(), expectedChannelString(), expectedFrameSize, getNumericDescriptor(), getSymbolicDescriptor(), type, ch, frameSize));
            }

            if (onCompletion != null) {
                onCompletion.run();
            } else {
                LOGGER.debug("No onCompletion action, doing nothing.");
            }
        } else {
            Object actualDescriptor = dt.getDescriptor();
            Object mappedDescriptor = FrameDescriptorMapping.lookupMapping(actualDescriptor);

            throw new IllegalArgumentException(String.format(
                "Frame was not as expected. Expected: " + "type=%s, channel=%s, descriptor=%s/%s but got: " + "type=%s, channel=%s, descriptor=%s(%s)",
                frameType.ordinal(), expectedChannelString(), getNumericDescriptor(), getSymbolicDescriptor(), type, ch, actualDescriptor, mappedDescriptor));
        }
    }

    private String expectedChannelString() {
        return expectedChannel == ANY_CHANNEL ? "<any>" : String.valueOf(expectedChannel);
    }

    @Override
    public Runnable getOnCompletionAction() {
        return onCompletion;
    }

    @Override
    public AbstractFrameFieldAndPayloadMatchingHandler onCompletion(Runnable onSuccessAction) {
        onCompletion = onSuccessAction;
        return this;
    }

    public AbstractFrameFieldAndPayloadMatchingHandler onChannel(int channel) {
        expectedChannel = channel;
        return this;
    }

    public int getActualChannel() {
        return actualChannel;
    }

    @Override
    public String toString() {
        return "AbstractFrameFieldAndPayloadMatchingHandler [_symbolicDescriptor=" + getSymbolicDescriptor() + ", _expectedChannel=" + expectedChannelString() + "]";
    }
}