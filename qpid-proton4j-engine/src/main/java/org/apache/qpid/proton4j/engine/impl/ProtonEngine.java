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
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;
import org.apache.qpid.proton4j.engine.AsyncEvent;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineSaslContext;
import org.apache.qpid.proton4j.engine.EngineState;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.exceptions.EngineClosedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineNotWritableException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.apache.qpid.proton4j.engine.exceptions.ProtonException;
import org.apache.qpid.proton4j.engine.exceptions.ProtonExceptionSupport;

/**
 * The default proton4j Engine implementation.
 */
public class ProtonEngine implements Engine {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonEngine.class);

    private final ProtonEnginePipeline pipeline =  new ProtonEnginePipeline(this);
    private final ProtonConnection connection = new ProtonConnection(this);
    private final ProtonEngineConfiguration configuration = new ProtonEngineConfiguration(this);

    private EngineSaslContext saslContext = new ProtonEngineNoOpSaslContext();

    private boolean writable;
    private EngineState state = EngineState.IDLE;

    private EventHandler<ProtonBuffer> outputHandler;
    private EventHandler<ProtonException> errorHandler;

    private ProtonFuture<Connection> connectionFuture = new ProtonFuture<>();

    private EventHandler<ProtonException> engineErrorHandler = (error) -> {
        LOG.warn("Engine encounted error and will shutdown: ", error);
    };

    ProtonConnection getConnection() {
        return connection;
    }

    @Override
    public boolean isWritable() {
        return writable;
    }

    @Override
    public boolean isShutdown() {
        return state == EngineState.SHUTDOWN;
    }

    @Override
    public EngineState state() {
        return state;
    }

    @Override
    public void start(EventHandler<AsyncEvent<Connection>> connectionReady) {
        if (connectionReady == null) {
            throw new NullPointerException("Start connection ready handler cannot be null");
        }

        state = EngineState.STARTING;
        try {
            pipeline().fireEngineStarting();
            // TODO - Should start fire the AMQP header and await that completion before returning a
            //        connection as ready ? I doesn't allow the Open to be pipelined behind the header
            //        but it would reduce complication of other bits of the state handling.
            connectionReady.handle(connectionFuture.onSuccess(getConnection()));
        } catch (Throwable error) {
            // TODO - Error types ?
            connectionReady.handle(connectionFuture.onFailure(error));
            state = EngineState.FAILED;
        }

        state = EngineState.STARTED;
        writable = true;
    }

    @Override
    public void shutdown() {
        state = EngineState.SHUTDOWN;
        writable = false;
    }

    @Override
    public long tick(long currentTime) {
        return 0;
    }

    @Override
    public void ingest(ProtonBuffer input) throws EngineStateException {
        // TODO - Check other states like closed.
        if (isShutdown()) {
            throw new EngineClosedException("The engine has already shut down.");
        }

        if (!isWritable()) {
            throw new EngineNotWritableException("Engine is currently not accepting new input");
        }

        try {
            pipeline.fireRead(input);
        } catch (Throwable t) {
            // TODO define what the pipeline does here as far as throwing vs signaling etc.
            engineFailed(ProtonExceptionSupport.create(t));
            throw t;
        }
    }

    //----- Engine configuration

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
    public ProtonEnginePipeline pipeline() {
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

    /**
     * Allows for registration of a custom {@link EngineSaslContext} that will convey
     * SASL state and configuration for this engine.
     *
     * @param saslContext
     *      The {@link EngineSaslContext} that this engine will use.
     *
     * @throws EngineStateException if the engine state doesn't allow for changes
     */
    public void registerSaslContext(EngineSaslContext saslContext) throws EngineStateException {
        if (state.ordinal() > EngineState.STARTING.ordinal()) {
            throw new EngineStateException("Cannot alter SASL context after engine has been started.");
        }

        this.saslContext = saslContext;
    }

    //----- Internal proton engine implementation

    void dispatchWriteToEventHandler(ProtonBuffer buffer) {
        if (outputHandler != null) {
            outputHandler.handle(buffer);
        } else {
            engineFailed(new EngineStateException("No output handler configured"));
        }
    }

    void engineFailed(ProtonException cause) {
        state = EngineState.FAILED;
        writable = false;
        engineErrorHandler.handle(cause);
    }
}
