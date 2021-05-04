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
package org.apache.qpid.protonj2.engine;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.types.security.SaslPerformative;
import org.apache.qpid.protonj2.types.security.SaslPerformative.SaslPerformativeHandler;
import org.apache.qpid.protonj2.types.transport.AMQPHeader.HeaderHandler;

/**
 * Frame object containing a SASL performative
 */
public class SASLEnvelope extends PerformativeEnvelope<SaslPerformative>{

    /**
     * The frame type marker used when decoding or encoding AMQP Header frames.
     */
    public static final byte SASL_FRAME_TYPE = (byte) 1;

    /**
     * Creates a new SASL envelope that wraps the given SASL performative.
     *
     * @param performative
     * 		The SASL performative to be wrapped.
     */
    public SASLEnvelope(SaslPerformative performative) {
        this(performative, null);
    }

    /**
     * Creates a new SASL envelope that wraps the given SASL performative.
     *
     * @param performative
     * 		The SASL performative to be wrapped.
     * @param payload
     * 		The payload that accompanies the wrapped SASL performative.
     */
    public SASLEnvelope(SaslPerformative performative, ProtonBuffer payload) {
        super(SASL_FRAME_TYPE);

        initialize(performative, 0, payload);
    }

    /**
     * Invoke the correct handler based on the SASL performative contained in this envelope.
     *
     * @param <E> The type of the context object that accompanies the invocation.
     *
     * @param handler
     * 		The {@link HeaderHandler} that should be invoked based on the performative type.
     * @param context
     * 		The context value to provide when signaling the SASL handler.
     */
    public <E> void invoke(SaslPerformativeHandler<E> handler, E context) {
        getBody().invoke(handler, context);
    }
}
