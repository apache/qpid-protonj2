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
package org.apache.qpid.proton4j.transport.impl;

import java.io.IOException;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonBufferAllocator;
import org.apache.qpid.proton4j.transport.ProtocolFrame;
import org.apache.qpid.proton4j.transport.Transport;
import org.apache.qpid.proton4j.transport.TransportListener;
import org.apache.qpid.proton4j.transport.TransportPipeline;

/**
 * The default Proton-J Transport implementation.
 */
public class ProtonTransport implements Transport {

    private int maxFrameSize = -1;
    private int initialMaxFrameSize = 4096;

    private final ProtonTransportPipeline pipeline;

    private ProtonBufferAllocator bufferAllocator;
    private TransportListener listener;

    public ProtonTransport() {
        this.pipeline = new ProtonTransportPipeline(this);
    }

    @Override
    public void processIncoming(ProtonBuffer buffer) throws IOException {
        pipeline.fireRead(buffer);
    }

    @Override
    public void setBufferAllocator(ProtonBufferAllocator allocator) {
        this.bufferAllocator = allocator;
    }

    @Override
    public ProtonBufferAllocator getBufferAllocator() {
        return bufferAllocator;
    }

    @Override
    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    @Override
    public void setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    public int getInitialMaxFrameSize() {
        return initialMaxFrameSize;
    }

    public void setInitialMaxFrameSize(int initialMaxFrameSize) {
        this.initialMaxFrameSize = initialMaxFrameSize;
    }

    @Override
    public TransportPipeline getPipeline() {
        return pipeline;
    }

    @Override
    public void write(ProtocolFrame frame) throws IOException {
        pipeline.fireWrite(frame);
    }

    @Override
    public void flush() {
        pipeline.fireFlush();
    }

    @Override
    public void setTransportListener(TransportListener listener) {
        this.listener = listener;
    }

    @Override
    public TransportListener getTransportListener() {
        return listener;
    }
}
