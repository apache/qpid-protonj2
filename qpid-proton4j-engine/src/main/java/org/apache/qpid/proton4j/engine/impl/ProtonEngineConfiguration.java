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
package org.apache.qpid.proton4j.engine.impl;

import org.apache.qpid.proton4j.buffer.ProtonBufferAllocator;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.EngineConfiguration;
import org.apache.qpid.proton4j.engine.EngineSaslContext.SaslState;

/**
 * Proton engine configuration API
 */
public class ProtonEngineConfiguration implements EngineConfiguration {

    private final ProtonEngine engine;

    private int maxFrameSize = ProtonConstants.MIN_MAX_AMQP_FRAME_SIZE;
    private ProtonBufferAllocator allocator = ProtonByteBufferAllocator.DEFAULT;

    private int remoteMaxFrameSize = ProtonConstants.MIN_MAX_AMQP_FRAME_SIZE;

    private int effectiveMaxInboundFrameSize = ProtonConstants.MIN_MAX_AMQP_FRAME_SIZE;
    private int effectiveMaxOutboundFrameSize = ProtonConstants.MIN_MAX_AMQP_FRAME_SIZE;

    ProtonEngineConfiguration(ProtonEngine engine) {
        this.engine = engine;
    }

    @Override
    public void setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSize = Math.max(ProtonConstants.MIN_MAX_AMQP_FRAME_SIZE, maxFrameSize);
    }

    @Override
    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    @Override
    public ProtonBufferAllocator getBufferAllocator() {
        return allocator;
    }

    @Override
    public void setBufferAllocator(ProtonBufferAllocator allocator) {
        this.allocator = allocator;
    }

    int getRemoteMaxFrameSize() {
        return remoteMaxFrameSize;
    }

    void setRemoteMaxFrameSize(int remoteMaxFrameSize) {
        this.remoteMaxFrameSize = remoteMaxFrameSize;
    }

    //---- proton4j specific APIs

    void recomputeEffectiveFrameSizeLimits() {
        // Based on engine state compute what the max in and out frame size should
        // be at this time.  Considerations to take into account are SASL state and
        // remote values once set.

        if (engine.saslContext().getSaslState().ordinal() < SaslState.AUTHENTICATED.ordinal()) {
            effectiveMaxInboundFrameSize = engine.saslContext().getMaxFrameSize();
            effectiveMaxOutboundFrameSize = engine.saslContext().getMaxFrameSize();
        } else {
            effectiveMaxInboundFrameSize = getMaxFrameSize();
            effectiveMaxOutboundFrameSize = Math.min(getMaxFrameSize(), remoteMaxFrameSize);
        }
    }

    int getOutboundMaxFrameSize() {
        return effectiveMaxOutboundFrameSize;
    }

    int getInboundMaxFrameSize() {
        return effectiveMaxInboundFrameSize;
    }
}
