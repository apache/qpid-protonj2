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
package org.apache.qpid.proton4j.engine.test;

import org.apache.qpid.proton4j.amqp.Binary;

public class FrameSender implements Runnable {

    private final EngineTestDriver testAmqpPeer;
    private final FrameType type;
    private final ListDescribedType frameDescribedType;
    private final Binary framePayload;
    private ValueProvider valueProvider;
    private int channel;
    private boolean deferWrite = false;
    private long sendDelay = 0;

    FrameSender(EngineTestDriver testAmqpPeer, FrameType type, int channel, ListDescribedType frameDescribedType, Binary framePayload) {
        this.testAmqpPeer = testAmqpPeer;
        this.type = type;
        this.channel = channel;
        this.frameDescribedType = frameDescribedType;
        this.framePayload = framePayload;
    }

    @Override
    public void run() {
        if (valueProvider != null) {
            valueProvider.setValues();
        }

        testAmqpPeer.sendFrame(type, channel, frameDescribedType, framePayload, deferWrite, sendDelay);
    }

    public FrameSender setValueProvider(ValueProvider valueProvider) {
        this.valueProvider = valueProvider;
        return this;
    }

    public FrameSender setChannel(int channel) {
        this.channel = channel;
        return this;
    }

    public FrameSender setDeferWrite(boolean deferWrite) {
        this.deferWrite = deferWrite;
        return this;
    }

    public void setSendDelay(long _sendDelay) {
        this.sendDelay = _sendDelay;
    }
}