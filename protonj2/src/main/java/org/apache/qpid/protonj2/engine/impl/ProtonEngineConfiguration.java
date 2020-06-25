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
package org.apache.qpid.protonj2.engine.impl;

import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.engine.EngineConfiguration;
import org.apache.qpid.protonj2.engine.EngineHandler;
import org.apache.qpid.protonj2.engine.EngineSaslDriver.SaslState;
import org.apache.qpid.protonj2.logging.ProtonLogger;
import org.apache.qpid.protonj2.logging.ProtonLoggerFactory;
import org.apache.qpid.protonj2.types.UnsignedInteger;

/**
 * Proton engine configuration API
 */
public class ProtonEngineConfiguration implements EngineConfiguration {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonEngineConfiguration.class);

    private final ProtonEngine engine;

    private ProtonBufferAllocator allocator = ProtonByteBufferAllocator.DEFAULT;

    private int effectiveMaxInboundFrameSize = ProtonConstants.MIN_MAX_AMQP_FRAME_SIZE;
    private int effectiveMaxOutboundFrameSize = ProtonConstants.MIN_MAX_AMQP_FRAME_SIZE;

    ProtonEngineConfiguration(ProtonEngine engine) {
        this.engine = engine;
    }

    @Override
    public ProtonBufferAllocator getBufferAllocator() {
        return allocator;
    }

    @Override
    public ProtonEngineConfiguration setBufferAllocator(ProtonBufferAllocator allocator) {
        this.allocator = allocator;
        return this;
    }


    @Override
    public EngineConfiguration setTraceFrames(boolean traceFrames) {
        // If the frame logging handler wasn't added or was removed for less overhead then
        // the setting will have no effect and isTraceFrames will always return false
        EngineHandler handler = engine.pipeline().find(ProtonConstants.FRAME_LOGGING_HANDLER);
        if (handler != null && handler instanceof ProtonFrameLoggingHandler) {
            ((ProtonFrameLoggingHandler) handler).setTraceFrames(traceFrames);
        } else {
            LOG.debug("Engine not configured with a frame logging handler: cannot apply traceFrames={}", traceFrames);
        }

        return this;
    }

    @Override
    public boolean isTraceFrames() {
        EngineHandler handler = engine.pipeline().find(ProtonConstants.FRAME_LOGGING_HANDLER);
        if (handler != null && handler instanceof ProtonFrameLoggingHandler) {
            return ((ProtonFrameLoggingHandler) handler).isTraceFrames();
        } else {
            return false;
        }
    }

    //---- proton specific APIs

    private static final long LONG_INT_MAX_VALUE = UnsignedInteger.valueOf(Integer.MAX_VALUE).longValue();

    void recomputeEffectiveFrameSizeLimits() {
        // Based on engine state compute what the max in and out frame size should
        // be at this time.  Considerations to take into account are SASL state and
        // remote values once set.

        if (engine.saslDriver().getSaslState().ordinal() < SaslState.AUTHENTICATED.ordinal()) {
            effectiveMaxInboundFrameSize = engine.saslDriver().getMaxFrameSize();
            effectiveMaxOutboundFrameSize = engine.saslDriver().getMaxFrameSize();
        } else {
            final long localMaxFrameSize = engine.connection().getMaxFrameSize();
            final long remoteMaxFrameSize = engine.connection().getRemoteMaxFrameSize();

            // TODO: Ignoring local set values over 4GB not that 2GB would work either.
            effectiveMaxInboundFrameSize = (int) Math.min(LONG_INT_MAX_VALUE, localMaxFrameSize);

            final long intermediateMaxOutboundFrameSize = Math.min(localMaxFrameSize, remoteMaxFrameSize);

            effectiveMaxOutboundFrameSize = (int) Math.min(LONG_INT_MAX_VALUE, intermediateMaxOutboundFrameSize);
        }
    }

    int getOutboundMaxFrameSize() {
        return effectiveMaxOutboundFrameSize;
    }

    int getInboundMaxFrameSize() {
        return effectiveMaxInboundFrameSize;
    }
}
