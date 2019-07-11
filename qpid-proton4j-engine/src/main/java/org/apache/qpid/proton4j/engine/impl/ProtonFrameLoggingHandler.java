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

/**
 * Handler that will log incoming and outgoing Frames
 */
public class ProtonFrameLoggingHandler implements EngineHandler {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonFrameLoggingHandler.class);

    private static final String PN_TRACE_FRM = "PN_TRACE_FRM";
    private static final boolean TRACE_FRM_ENABLED = checkTraceFramesEnabled();

    private static final boolean checkTraceFramesEnabled()
    {
        String value = System.getenv(PN_TRACE_FRM);
        return "true".equalsIgnoreCase(value) ||
            "1".equals(value) ||
            "yes".equalsIgnoreCase(value);
    }

    // TODO - Possible that this should also have configuration for on / off and even looks at
    //        env for PN_TRACE_FRM for legacy reasons (hacky version of that since added).

    // TODO - Implement the frame logging where now we are only stringifying the frames.

    @Override
    public void handlerAdded(EngineHandlerContext context) throws Exception {
    }

    @Override
    public void handlerRemoved(EngineHandlerContext context) throws Exception {
    }

    @Override
    public void handleRead(EngineHandlerContext context, HeaderFrame header) {
        if(TRACE_FRM_ENABLED) {
            System.out.println("<- " + header.getBody());
        }

        LOG.trace("<- {}", header.getBody());
        context.fireRead(header);
    }

    @Override
    public void handleRead(EngineHandlerContext context, SaslFrame frame) {
        if(TRACE_FRM_ENABLED) {
            System.out.println("<- SASL: " + frame.getBody());
        }

        LOG.trace("<- SASL: {}", frame.getBody());
        context.fireRead(frame);
    }

    @Override
    public void handleRead(EngineHandlerContext context, ProtocolFrame frame) {
        if(TRACE_FRM_ENABLED) {
            System.out.println("<- AMQP: " + frame.getBody());
        }

        LOG.trace("<- AMQP: {}", frame.getBody());
        context.fireRead(frame);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, AMQPHeader header) {
        if(TRACE_FRM_ENABLED) {
            System.out.println("-> AMQP: " + header);
        }

        LOG.trace("-> AMQP: {}", header);
        context.fireWrite(header);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
        if(TRACE_FRM_ENABLED) {
            System.out.println("-> AMQP: " + performative);  // TODO - Payload ?
        }

        LOG.trace("-> AMQP: {}", performative);  // TODO - Payload ?
        context.fireWrite(performative, channel, payload, payloadToLarge);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SaslPerformative performative) {
        if(TRACE_FRM_ENABLED) {
            System.out.println("-> SASL: " + performative);
        }

        LOG.trace("-> SASL: {}", performative);
        context.fireWrite(performative);
    }
}
