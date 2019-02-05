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

/**
 * Factory class for proton4j Engine creation
 */
public final class ProtonEngineFactory {

    private ProtonEngineFactory() {}

    // TODO - How do we distinguish client from server, or should we ?

    public static ProtonEngine createClientEngine() {
        ProtonEngine engine = new ProtonEngine();

        ProtonEnginePipeline pipeline = engine.pipeline();

        pipeline.addLast(ProtonConstants.AMQP_PERFORMATIVE_HANDLER_NAME, new ProtonPerformativeHandler());
        pipeline.addLast(ProtonConstants.FRAME_LOGGING_HANDLER, new ProtonFrameLoggingHandler());
        pipeline.addLast(ProtonConstants.FRAME_PARSING_HANDLER, new ProtonFrameParsingHandler());
        pipeline.addLast(ProtonConstants.FRAME_WRITING_HANDLER, new ProtonFrameWritingHandler());

        return engine;
    }

    public static ProtonEngine createServierEngine() {
        ProtonEngine engine = new ProtonEngine();

        ProtonEnginePipeline pipeline = engine.pipeline();

        pipeline.addLast(ProtonConstants.AMQP_PERFORMATIVE_HANDLER_NAME, new ProtonPerformativeHandler());
        pipeline.addLast(ProtonConstants.FRAME_LOGGING_HANDLER, new ProtonFrameLoggingHandler());
        pipeline.addLast(ProtonConstants.FRAME_PARSING_HANDLER, new ProtonFrameParsingHandler());
        pipeline.addLast(ProtonConstants.FRAME_WRITING_HANDLER, new ProtonFrameWritingHandler());

        return engine;
    }
}
