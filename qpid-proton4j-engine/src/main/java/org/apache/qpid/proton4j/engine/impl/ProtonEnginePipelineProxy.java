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

import java.util.Objects;

import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineHandler;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.EnginePipeline;
import org.apache.qpid.proton4j.engine.HeaderFrame;
import org.apache.qpid.proton4j.engine.ProtocolFrame;
import org.apache.qpid.proton4j.engine.SaslFrame;
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineNotWritableException;

/**
 * Wrapper around the internal {@link ProtonEnginePipeline} used to present a guarded
 * pipeline to the outside world when the {@link Engine#pipeline()} method is used
 * to gain access to the pipeline.  The proxy will ensure that any read or write
 * calls enforce {@link Engine} state such as not started and shutdown.
 */
public class ProtonEnginePipelineProxy implements EnginePipeline {

    private final ProtonEnginePipeline pipeline;

    ProtonEnginePipelineProxy(ProtonEnginePipeline pipeline) {
        Objects.requireNonNull(pipeline, "Must supply a real pipline instance to wrap.");
        this.pipeline = pipeline;
    }

    @Override
    public ProtonEngine engine() {
        return pipeline.engine();
    }

    /**
     * @return the wrapped {@link ProtonEnginePipeline} for testing.
     */
    ProtonEnginePipeline pipeline() {
        return pipeline;
    }

    @Override
    public ProtonEnginePipelineProxy addFirst(String name, EngineHandler handler) {
        engine().checkShutdownOrFailed("Cannot add pipeline resources when Engine is shutdown or failed");
        pipeline.addFirst(name, handler);
        return this;
    }

    @Override
    public ProtonEnginePipelineProxy addLast(String name, EngineHandler handler) {
        engine().checkShutdownOrFailed("Cannot add pipeline resources when Engine is shutdown or failed");
        pipeline.addLast(name, handler);
        return this;
    }

    @Override
    public ProtonEnginePipelineProxy removeFirst() {
        pipeline.removeFirst();
        return this;
    }

    @Override
    public ProtonEnginePipelineProxy removeLast() {
        pipeline.removeLast();
        return this;
    }

    @Override
    public ProtonEnginePipelineProxy remove(String name) {
        pipeline.remove(name);
        return this;
    }

    @Override
    public EnginePipeline remove(EngineHandler handler) {
        pipeline.remove(handler);
        return this;
    }

    @Override
    public EngineHandler find(String name) {
        engine().checkShutdownOrFailed("Cannot access pipeline resource when Engine is shutdown or failed");
        return pipeline.find(name);
    }

    @Override
    public EngineHandler first() {
        engine().checkShutdownOrFailed("Cannot access pipeline resource when Engine is shutdown or failed");
        return pipeline.first();
    }

    @Override
    public EngineHandler last() {
        engine().checkShutdownOrFailed("Cannot access pipeline resource when Engine is shutdown or failed");
        return pipeline.last();
    }

    @Override
    public EngineHandlerContext firstContext() {
        engine().checkShutdownOrFailed("Cannot access pipeline resource when Engine is shutdown or failed");
        return pipeline.firstContext();
    }

    @Override
    public EngineHandlerContext lastContext() {
        engine().checkShutdownOrFailed("Cannot access pipeline resource when Engine is shutdown or failed");
        return pipeline.lastContext();
    }

    //----- Event injection methods

    @Override
    public ProtonEnginePipelineProxy fireEngineStarting() {
        throw new IllegalAccessError("Cannot trigger starting on Egnine owned Pipeline resource.");
    }

    @Override
    public ProtonEnginePipelineProxy fireEngineStateChanged() {
        throw new IllegalAccessError("Cannot trigger state changed on Egnine owned Pipeline resource.");
    }

    @Override
    public ProtonEnginePipelineProxy fireFailed(EngineFailedException e) {
        throw new IllegalAccessError("Cannot trigger failed on Egnine owned Pipeline resource.");
    }

    @Override
    public ProtonEnginePipelineProxy fireRead(ProtonBuffer input) {
        engine().checkEngineNotStarted("Cannot inject new data into an unstarted Engine");
        engine().checkShutdownOrFailed("Cannot inject new data into an Engine that is shutdown or failed");
        pipeline.fireRead(input);
        return this;
    }

    @Override
    public ProtonEnginePipelineProxy fireRead(HeaderFrame header) {
        engine().checkEngineNotStarted("Cannot inject new data into an unstarted Engine");
        engine().checkShutdownOrFailed("Cannot inject new data into an Engine that is shutdown or failed");
        pipeline.fireRead(header);
        return this;
    }

    @Override
    public ProtonEnginePipelineProxy fireRead(SaslFrame frame) {
        engine().checkEngineNotStarted("Cannot inject new data into an unstarted Engine");
        engine().checkShutdownOrFailed("Cannot inject new data into an Engine that is shutdown or failed");
        pipeline.fireRead(frame);
        return this;
    }

    @Override
    public ProtonEnginePipelineProxy fireRead(ProtocolFrame frame) {
        engine().checkEngineNotStarted("Cannot inject new data into an unstarted Engine");
        engine().checkShutdownOrFailed("Cannot inject new data into an Engine that is shutdown or failed");
        pipeline.fireRead(frame);
        return this;
    }

    @Override
    public ProtonEnginePipelineProxy fireWrite(AMQPHeader header) {
        engine().checkEngineNotStarted("Cannot write from an unstarted Engine");
        engine().checkShutdownOrFailed("Cannot write form an Engine that is shutdown or failed");

        if (!engine().isWritable()) {
            throw new EngineNotWritableException("Cannot write through Engine pipeline when Engine is not writable");
        }

        pipeline.fireWrite(header);
        return this;
    }

    @Override
    public ProtonEnginePipelineProxy fireWrite(Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
        engine().checkEngineNotStarted("Cannot write from an unstarted Engine");
        engine().checkShutdownOrFailed("Cannot write form an Engine that is shutdown or failed");

        if (!engine().isWritable()) {
            throw new EngineNotWritableException("Cannot write through Engine pipeline when Engine is not writable");
        }

        pipeline.fireWrite(performative, channel, payload, payloadToLarge);
        return this;
    }

    @Override
    public ProtonEnginePipelineProxy fireWrite(SaslPerformative performative) {
        engine().checkEngineNotStarted("Cannot write from an unstarted Engine");
        engine().checkShutdownOrFailed("Cannot write form an Engine that is shutdown or failed");

        if (!engine().isWritable()) {
            throw new EngineNotWritableException("Cannot write through Engine pipeline when Engine is not writable");
        }

        pipeline.fireWrite(performative);
        return this;
    }

    @Override
    public ProtonEnginePipelineProxy fireWrite(ProtonBuffer buffer) {
        engine().checkEngineNotStarted("Cannot write from an unstarted Engine");
        engine().checkShutdownOrFailed("Cannot write form an Engine that is shutdown or failed");

        if (!engine().isWritable()) {
            throw new EngineNotWritableException("Cannot write through Engine pipeline when Engine is not writable");
        }

        pipeline.fireWrite(buffer);
        return this;
    }
}
