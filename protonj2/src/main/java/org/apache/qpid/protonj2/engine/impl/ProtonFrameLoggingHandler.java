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
import org.apache.qpid.protonj2.engine.HeaderEnvelope;
import org.apache.qpid.protonj2.engine.IncomingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.OutgoingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.SASLEnvelope;
import org.apache.qpid.protonj2.engine.util.StringUtils;
import org.apache.qpid.protonj2.logging.ProtonLogger;
import org.apache.qpid.protonj2.logging.ProtonLoggerFactory;

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
    private int uniqueIdentifier;

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
    public void handlerAdded(EngineHandlerContext context) {
        // Provides a stable Id for the handler to use when logging frame traces so that applications with
        // multiple connections can be more easily debugged.
        uniqueIdentifier = System.identityHashCode(context.engine());
    }

    @Override
    public void handleRead(EngineHandlerContext context, HeaderEnvelope envelope) {
        if (traceFrames) {
            trace(envelope.isSaslHeader() ? SASL_OUT_PREFIX : AMQP_OUT_PREFIX, uniqueIdentifier, 0, envelope.getBody(), null);
        }

        log(envelope.isSaslHeader() ? SASL_OUT_PREFIX : AMQP_OUT_PREFIX, uniqueIdentifier, 0, envelope.getBody(), null);

        context.fireRead(envelope);
    }

    @Override
    public void handleRead(EngineHandlerContext context, SASLEnvelope envelope) {
        if (traceFrames) {
            trace(SASL_IN_PREFIX, uniqueIdentifier, 0, envelope.getBody(), null);
        }

        log(SASL_IN_PREFIX, uniqueIdentifier, 0, envelope.getBody(), envelope.getPayload());

        context.fireRead(envelope);
    }

    @Override
    public void handleRead(EngineHandlerContext context, IncomingAMQPEnvelope envelope) {
        if (traceFrames) {
            trace(AMQP_IN_PREFIX, uniqueIdentifier, envelope.getChannel(), envelope.getBody(), envelope.getPayload());
        }

        if (LOG.isTraceEnabled()) {
            log(AMQP_IN_PREFIX, uniqueIdentifier, envelope.getChannel(), envelope.getBody(), envelope.getPayload());
        }

        context.fireRead(envelope);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, HeaderEnvelope envelope) {
        if (traceFrames) {
            trace(envelope.isSaslHeader() ? SASL_OUT_PREFIX : AMQP_OUT_PREFIX, uniqueIdentifier, 0, envelope.getBody(), null);
        }

        log(envelope.isSaslHeader() ? SASL_OUT_PREFIX : AMQP_OUT_PREFIX, uniqueIdentifier, 0, envelope.getBody(), null);

        context.fireWrite(envelope);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, OutgoingAMQPEnvelope envelope) {
        if (traceFrames) {
            trace(AMQP_OUT_PREFIX, uniqueIdentifier, envelope.getChannel(), envelope.getBody(), envelope.getPayload());
        }

        if (LOG.isTraceEnabled()) {
            log(AMQP_OUT_PREFIX, uniqueIdentifier, envelope.getChannel(), envelope.getBody(), envelope.getPayload());
        }

        context.fireWrite(envelope);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SASLEnvelope envelope) {
        if (traceFrames) {
            trace(SASL_OUT_PREFIX, uniqueIdentifier, 0, envelope.getBody(), null);
        }

        log(SASL_OUT_PREFIX, uniqueIdentifier, 0, envelope.getBody(), null);

        context.fireWrite(envelope);
    }

    private static final void log(String prefix, int connection, int channel, Object performative, ProtonBuffer payload) {
        if (payload == null) {
            LOG.trace("{}:[{}:{}] {}", prefix, connection, channel, performative);
        } else {
            LOG.trace("{}:[{}:{}] {} - {}", prefix, connection, performative, StringUtils.toQuotedString(payload, PAYLOAD_STRING_LIMIT, true));
        }
    }

    private static final void trace(String prefix, int connection, int channel, Object performative, ProtonBuffer payload) {
        if (payload == null) {
            System.out.println(String.format("%s:[%d:%d] %s", prefix, connection, channel, performative));
        } else {
            System.out.println(String.format("%s:[%d:%d] %s - %s", prefix, connection, channel, performative, StringUtils.toQuotedString(payload, PAYLOAD_STRING_LIMIT, true)));
        }
    }
}
