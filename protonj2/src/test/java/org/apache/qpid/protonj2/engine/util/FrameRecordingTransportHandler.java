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
import org.apache.qpid.protonj2.engine.Frame;
import org.apache.qpid.protonj2.engine.HeaderFrame;
import org.apache.qpid.protonj2.engine.IncomingProtocolFrame;
import org.apache.qpid.protonj2.engine.OutgoingProtocolFrame;
import org.apache.qpid.protonj2.engine.SaslFrame;

public class FrameRecordingTransportHandler implements EngineHandler {

    private List<Frame<?>> framesRead = new ArrayList<>();
    private List<Frame<?>> framesWritten = new ArrayList<>();

    public FrameRecordingTransportHandler() {
    }

    public List<Frame<?>> getFramesWritten() {
        return framesWritten;
    }

    public List<Frame<?>> getFramesRead() {
        return framesRead;
    }

    @Override
    public void handleRead(EngineHandlerContext context, HeaderFrame header) {
        framesRead.add(header);
        context.fireRead(header);
    }

    @Override
    public void handleRead(EngineHandlerContext context, SaslFrame frame) {
        framesRead.add(frame);
        context.fireRead(frame);
    }

    @Override
    public void handleRead(EngineHandlerContext context, IncomingProtocolFrame frame) {
        framesRead.add(frame);
        context.fireRead(frame);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, HeaderFrame frame) {
        framesWritten.add(frame);
        context.fireWrite(frame);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, OutgoingProtocolFrame frame) {
        framesWritten.add(frame);
        context.fireWrite(frame);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SaslFrame frame) {
        framesWritten.add(frame);
        context.fireWrite(frame);
    }
}
