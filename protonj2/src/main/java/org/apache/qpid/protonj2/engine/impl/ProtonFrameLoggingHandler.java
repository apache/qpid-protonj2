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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.EngineHandler;
import org.apache.qpid.protonj2.engine.EngineHandlerContext;
import org.apache.qpid.protonj2.engine.HeaderFrame;
import org.apache.qpid.protonj2.engine.ProtocolFrame;
import org.apache.qpid.protonj2.engine.SaslFrame;
import org.apache.qpid.protonj2.engine.util.StringUtils;
import org.apache.qpid.protonj2.logging.ProtonLogger;
import org.apache.qpid.protonj2.logging.ProtonLoggerFactory;
import org.apache.qpid.protonj2.types.security.SaslPerformative;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;
import org.apache.qpid.protonj2.types.transport.Performative;

/**
 * Handler that will log incoming and outgoing Frames
 */
public class ProtonFrameLoggingHandler implements EngineHandler {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonFrameLoggingHandler.class);

    private static final String AMQP_IN_PREFIX = "<- AMQP";
    private static final String AMQP_OUT_PREFIX = "-> AMQP";
    private static final String SASL_IN_PREFIX = "<- SASL";
    private static final String SASL_OUT_PREFIX = "-> SASL";

    private static final int PAYLOAD_STRING_LIMIT = 64;
    private static final String PN_TRACE_FRM = "PN_TRACE_FRM";
    private static final boolean TRACE_FRM_ENABLED = checkTraceFramesEnabled();

    private boolean traceFrames = TRACE_FRM_ENABLED;

    private static final boolean checkTraceFramesEnabled() {
        String value = System.getenv(PN_TRACE_FRM);
        return "true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value);
    }

    void setTraceFrames(boolean traceFrames) {
        this.traceFrames = traceFrames;
    }

    boolean isTraceFrames() {
        return traceFrames;
    }

    @Override
    public void handleRead(EngineHandlerContext context, HeaderFrame header) {
        if (traceFrames) {
            trace(AMQP_IN_PREFIX, header.getBody(), null);
        }

        log(AMQP_IN_PREFIX, header.getBody(), null);

        context.fireRead(header);
    }

    @Override
    public void handleRead(EngineHandlerContext context, SaslFrame frame) {
        if (traceFrames) {
            trace(SASL_IN_PREFIX, frame.getBody(), null);
        }

        log(SASL_IN_PREFIX, frame.getBody(), frame.getPayload());

        context.fireRead(frame);
    }

    @Override
    public void handleRead(EngineHandlerContext context, ProtocolFrame frame) {
        if (traceFrames) {
            trace(AMQP_IN_PREFIX, frame.getBody(), frame.getPayload());
        }

        if (LOG.isTraceEnabled()) {
            log(AMQP_IN_PREFIX, frame.getBody(), frame.getPayload());
        }

        context.fireRead(frame);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, AMQPHeader header) {
        if (traceFrames) {
            trace(AMQP_OUT_PREFIX, header, null);
        }

        log(AMQP_OUT_PREFIX, header, null);

        context.fireWrite(header);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
        if (traceFrames) {
            trace(AMQP_OUT_PREFIX, performative, payload);
        }

        if (LOG.isTraceEnabled()) {
            log(AMQP_OUT_PREFIX, performative, payload);
        }

        context.fireWrite(performative, channel, payload, payloadToLarge);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SaslPerformative performative) {
        if (traceFrames) {
            trace(SASL_OUT_PREFIX, performative, null);
        }

        log(SASL_OUT_PREFIX, performative, null);

        context.fireWrite(performative);
    }

    private static final void log(String prefix, Object performative, ProtonBuffer payload) {
        if (payload == null) {
            LOG.trace("{}: {}", prefix, performative);
        } else {
            LOG.trace("{}: {} - {}", prefix, performative, StringUtils.toQuotedString(payload, PAYLOAD_STRING_LIMIT, true));
        }
    }

    private static final void trace(String prefix, Object performative, ProtonBuffer payload) {
        if (payload == null) {
            System.out.println(prefix + ": " + performative);
        } else {
            System.out.println(prefix + ": " + performative + " - " + StringUtils.toQuotedString(payload, PAYLOAD_STRING_LIMIT, true));
        }
    }
}
