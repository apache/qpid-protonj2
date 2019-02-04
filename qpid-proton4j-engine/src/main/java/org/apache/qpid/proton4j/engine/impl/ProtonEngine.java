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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.AsyncResult;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EnginePipeline;
import org.apache.qpid.proton4j.engine.EngineSaslContext;
import org.apache.qpid.proton4j.engine.EngineState;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.exceptions.ProtonException;

/**
 * The default proton4j Engine implementation.
 */
public class ProtonEngine implements Engine {

    private final ProtonEnginePipeline pipeline;

    private EngineSaslContext saslContext = new ProtonEngineNoOpSaslContext();
    private ProtonEngineConfiguration configuration;

    private boolean writable;
    private EngineState state = EngineState.IDLE;

    private EventHandler<ProtonBuffer> outputHandler;
    private EventHandler<ProtonException> errorHandler;

    public ProtonEngine() {
        this.pipeline = new ProtonEnginePipeline(this);
    }

    @Override
    public boolean isWritable() {
        return writable;
    }

    @Override
    public EngineState state() {
        return state;
    }

    @Override
    public void start(EventHandler<AsyncResult<Connection>> handler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void shutdown() {
        state = EngineState.SHUTDOWN;
        // TODO Auto-generated method stub
    }

    @Override
    public void ingest(ProtonBuffer input) {
        // TODO - Error handling ?
        pipeline.fireRead(input);
    }

    //----- Transport configuration ------------------------------------------//

    @Override
    public void outputHandler(EventHandler<ProtonBuffer> handler) {
        this.outputHandler = handler;
    }

    EventHandler<ProtonBuffer> outputHandler() {
        return outputHandler;
    }

    @Override
    public void errorHandler(EventHandler<ProtonException> handler) {
        this.errorHandler = handler;
    }

    EventHandler<ProtonException> errorHandler() {
        return errorHandler;
    }

    @Override
    public EnginePipeline pipeline() {
        return pipeline;
    }

    @Override
    public ProtonEngineConfiguration configuration() {
        return configuration;
    }

    @Override
    public EngineSaslContext saslContext() {
        return saslContext;
    }
}
