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

import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonBufferAllocator;
import org.apache.qpid.proton4j.transport.FrameParser;
import org.apache.qpid.proton4j.transport.ProtocolTracer;
import org.apache.qpid.proton4j.transport.Transport;
import org.apache.qpid.proton4j.transport.TransportListener;

/**
 * The default Proton-J Transport implementation.
 */
public class ProtonTransport implements Transport {

    @Override
    public void processIncoming(ProtonBuffer buffer) throws IOException {
    }

    @Override
    public void setBufferAllocator(ProtonBufferAllocator allocator) {
    }

    @Override
    public ProtonBufferAllocator getBufferAllocator() {
        return null;
    }

    @Override
    public void setTransportListener(TransportListener listener) {
    }

    @Override
    public TransportListener getTransportListener() {
        return null;
    }

    @Override
    public void setProtocolTracer(ProtocolTracer tracer) {
    }

    @Override
    public ProtocolTracer getProtocolTracer() {
        return null;
    }

    @Override
    public int getMaxFrameSize() {
        return 0;
    }

    @Override
    public void setMaxFrameSize(int size) {
    }

    /**
     * Handles receipt of an AMQP Header from the incoming data stream
     * <p>
     * The method is called on successful decode of an AMQP Header giving the
     * Transport instance a chance to alter its state to reflect the processing
     * that should occur following the type of header read.  The caller provides
     * the buffer where the header was read from so that any additional frames
     * can be decoded using an appropriate {@link FrameParser} instance.
     *
     * @param header
     *      The newly decoded AMQP Header instance from the data stream
     * @param buffer
     *      The incoming buffer where the header was read.
     */
    public void onAMQPHeader(AMQPHeader header, ProtonBuffer buffer) {

    }
}
