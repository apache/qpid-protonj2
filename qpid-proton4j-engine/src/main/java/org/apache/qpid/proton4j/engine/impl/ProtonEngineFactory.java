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

import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineFactory;
import org.apache.qpid.proton4j.engine.impl.sasl.ProtonSaslHandler;

/**
 * Factory class for proton4j Engine creation
 */
public final class ProtonEngineFactory implements EngineFactory {

    @Override
    public Engine createEngine() {
        ProtonEngine engine = new ProtonEngine();
        ProtonEnginePipeline pipeline = engine.pipeline();

        pipeline.addLast(ProtonConstants.ENGINE_NOT_STARTED_WRITE_GATE, ProtonEngineNotStartedHandler.INSTANCE);
        pipeline.addLast(ProtonConstants.AMQP_PERFORMATIVE_HANDLER, new ProtonPerformativeHandler());
        pipeline.addLast(ProtonConstants.SASL_PERFORMATIVE_HANDLER, new ProtonSaslHandler());
        pipeline.addLast(ProtonConstants.FRAME_LOGGING_HANDLER, new ProtonFrameLoggingHandler());
        pipeline.addLast(ProtonConstants.FRAME_DECODING_HANDLER, new ProtonFrameDecodingHandler());
        pipeline.addLast(ProtonConstants.FRAME_ENCODING_HANDLER, new ProtonFrameEncodingHandler());
        pipeline.addLast(ProtonConstants.ENGINE_NOT_STARTED_READ_GATE, ProtonEngineNotStartedHandler.INSTANCE);

        return engine;
   }

    @Override
    public Engine createNonSaslEngine() {
        ProtonEngine engine = new ProtonEngine();
        ProtonEnginePipeline pipeline = engine.pipeline();

        pipeline.addLast(ProtonConstants.ENGINE_NOT_STARTED_WRITE_GATE, ProtonEngineNotStartedHandler.INSTANCE);
        pipeline.addLast(ProtonConstants.AMQP_PERFORMATIVE_HANDLER, new ProtonPerformativeHandler());
        pipeline.addLast(ProtonConstants.FRAME_LOGGING_HANDLER, new ProtonFrameLoggingHandler());
        pipeline.addLast(ProtonConstants.FRAME_DECODING_HANDLER, new ProtonFrameDecodingHandler());
        pipeline.addLast(ProtonConstants.FRAME_ENCODING_HANDLER, new ProtonFrameEncodingHandler());
        pipeline.addLast(ProtonConstants.ENGINE_NOT_STARTED_READ_GATE, ProtonEngineNotStartedHandler.INSTANCE);

        return engine;
    }
}
