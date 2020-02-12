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

import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;
import org.apache.qpid.proton4j.engine.EngineHandler;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.HeaderFrame;
import org.apache.qpid.proton4j.engine.ProtocolFrame;
import org.apache.qpid.proton4j.engine.SaslFrame;
import org.apache.qpid.proton4j.engine.util.StringUtils;

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

    private static final boolean checkTraceFramesEnabled() {
        String value = System.getenv(PN_TRACE_FRM);
        return "true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value);
    }

    @Override
    public void handleRead(EngineHandlerContext context, HeaderFrame header) {
        if (TRACE_FRM_ENABLED) {
            trace(AMQP_IN_PREFIX, header.getBody(), null);
        }

        log(AMQP_IN_PREFIX, header.getBody(), null);

        context.fireRead(header);
    }

    @Override
    public void handleRead(EngineHandlerContext context, SaslFrame frame) {
        if (TRACE_FRM_ENABLED) {
            trace(SASL_IN_PREFIX, frame.getBody(), null);
        }

        log(SASL_IN_PREFIX, frame.getBody(), frame.getPayload());

        context.fireRead(frame);
    }

    @Override
    public void handleRead(EngineHandlerContext context, ProtocolFrame frame) {
        if (TRACE_FRM_ENABLED) {
            trace(AMQP_IN_PREFIX, frame.getBody(), frame.getPayload());
        }

        if (LOG.isTraceEnabled()) {
            log(AMQP_IN_PREFIX, frame.getBody(), frame.getPayload());
        }

        context.fireRead(frame);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, AMQPHeader header) {
        if (TRACE_FRM_ENABLED) {
            trace(AMQP_OUT_PREFIX, header, null);
        }

        log(AMQP_OUT_PREFIX, header, null);

        context.fireWrite(header);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
        if (TRACE_FRM_ENABLED) {
            trace(AMQP_OUT_PREFIX, performative, payload);
        }

        if (LOG.isTraceEnabled()) {
            log(AMQP_OUT_PREFIX, performative, payload);
        }

        context.fireWrite(performative, channel, payload, payloadToLarge);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SaslPerformative performative) {
        if (TRACE_FRM_ENABLED) {
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
