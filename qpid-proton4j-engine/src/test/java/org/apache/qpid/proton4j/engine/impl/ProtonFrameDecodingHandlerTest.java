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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.HeaderFrame;
import org.apache.qpid.proton4j.engine.util.TestSupportTransportHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ProtonFrameDecodingHandlerTest {

    private TestSupportTransportHandler testHandler;

    @Before
    public void setUp() {
        testHandler = new TestSupportTransportHandler();
    }

    @Test
    public void testDecodeValidHeaderTriggersHeaderRead() {
        Engine engine = createEngine();

        // Check for Header processing
        engine.pipeline().fireRead(AMQPHeader.getAMQPHeader().getBuffer());

        Object frame = testHandler.getFramesRead().get(0);
        assertTrue(frame instanceof HeaderFrame);
        HeaderFrame header = (HeaderFrame) frame;
        assertEquals(AMQPHeader.getAMQPHeader(), header.getBody());
    }

    @Test
    public void testReadValidHeaderInSingleByteChunks() {
        ProtonFrameDecodingHandler handler = new ProtonFrameDecodingHandler();
        EngineHandlerContext context = Mockito.mock(EngineHandlerContext.class);

        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 'A' }));
        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 'M' }));
        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 'Q' }));
        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 'P' }));
        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 0 }));
        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 1 }));
        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 0 }));
        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 0 }));

        Mockito.verify(context).fireRead(Mockito.any(HeaderFrame.class));
        Mockito.verifyNoMoreInteractions(context);
    }

    @Test
    public void testReadValidHeaderInSplitChunks() {
        ProtonFrameDecodingHandler handler = new ProtonFrameDecodingHandler();
        EngineHandlerContext context = Mockito.mock(EngineHandlerContext.class);

        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 'A', 'M', 'Q', 'P' }));
        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 0, 1, 0, 0 }));

        Mockito.verify(context).fireRead(Mockito.any(HeaderFrame.class));
        Mockito.verifyNoMoreInteractions(context);
    }

    @Test
    public void testDecodeValidSaslHeaderTriggersHeaderRead() {
        Engine engine = createEngine();

        // Check for Header processing
        engine.pipeline().fireRead(AMQPHeader.getSASLHeader().getBuffer());

        Object frame = testHandler.getFramesRead().get(0);
        assertTrue(frame instanceof HeaderFrame);
        HeaderFrame header = (HeaderFrame) frame;
        assertEquals(AMQPHeader.getSASLHeader(), header.getBody());
    }

    @Test
    public void testInvalidHeaderBytesTriggersError() {
        ProtonFrameDecodingHandler handler = new ProtonFrameDecodingHandler();
        EngineHandlerContext context = Mockito.mock(EngineHandlerContext.class);

        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] { 'S' }));

        Mockito.verify(context).fireDecodingError(Mockito.any(IOException.class));
        Mockito.verifyNoMoreInteractions(context);

        // Verify that the parser accepts no new input once in error state.
        Mockito.clearInvocations(context);
        handler.handleRead(context, AMQPHeader.getSASLHeader().getBuffer());
        Mockito.verify(context).fireDecodingError(Mockito.any(IOException.class));
    }

    private Engine createEngine() {
        ProtonEngine transport = new ProtonEngine();

        transport.pipeline().addLast("test", testHandler);
        transport.pipeline().addLast("frames", new ProtonFrameDecodingHandler());

        return transport;
    }
}
