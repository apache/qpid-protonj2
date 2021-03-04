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
package org.apache.qpid.protonj2.engine.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.protonj2.engine.EngineHandler;
import org.apache.qpid.protonj2.engine.EngineHandlerContext;
import org.apache.qpid.protonj2.engine.PerformativeEnvelope;
import org.apache.qpid.protonj2.engine.HeaderEnvelope;
import org.apache.qpid.protonj2.engine.IncomingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.OutgoingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.SASLEnvelope;

public class FrameRecordingTransportHandler implements EngineHandler {

    private List<PerformativeEnvelope<?>> framesRead = new ArrayList<>();
    private List<PerformativeEnvelope<?>> framesWritten = new ArrayList<>();

    public FrameRecordingTransportHandler() {
    }

    public List<PerformativeEnvelope<?>> getFramesWritten() {
        return framesWritten;
    }

    public List<PerformativeEnvelope<?>> getFramesRead() {
        return framesRead;
    }

    @Override
    public void handleRead(EngineHandlerContext context, HeaderEnvelope header) {
        framesRead.add(header);
        context.fireRead(header);
    }

    @Override
    public void handleRead(EngineHandlerContext context, SASLEnvelope frame) {
        framesRead.add(frame);
        context.fireRead(frame);
    }

    @Override
    public void handleRead(EngineHandlerContext context, IncomingAMQPEnvelope frame) {
        framesRead.add(frame);
        context.fireRead(frame);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, HeaderEnvelope frame) {
        framesWritten.add(frame);
        context.fireWrite(frame);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, OutgoingAMQPEnvelope frame) {
        framesWritten.add(frame);
        context.fireWrite(frame);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SASLEnvelope frame) {
        framesWritten.add(frame);
        context.fireWrite(frame);
    }
}
