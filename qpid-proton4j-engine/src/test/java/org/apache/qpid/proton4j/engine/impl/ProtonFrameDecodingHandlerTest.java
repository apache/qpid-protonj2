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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.EmptyFrame;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.HeaderFrame;
import org.apache.qpid.proton4j.engine.ProtocolFrame;
import org.apache.qpid.proton4j.engine.util.TestSupportTransportHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
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

    @Test
    public void testDecodeEmptyOpenEncodedFrame() throws Exception {
        // Frame data for: Open
        //   Open{ containerId='null', hostname='null', maxFrameSize=4294967295, channelMax=65535,
        //         idleTimeOut=null, outgoingLocales=null, incomingLocales=null, offeredCapabilities=null,
        //         desiredCapabilities=null, properties=null}
        final byte[] emptyOpen = new byte[] {0, 0, 0, 15, 2, 0, 0, 0, 0, 83, 16, -64, 2, 1, 64};

        ArgumentCaptor<ProtocolFrame> argument = ArgumentCaptor.forClass(ProtocolFrame.class);

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        EngineHandlerContext context = Mockito.mock(EngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());
        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(emptyOpen));

        Mockito.verify(context).fireRead(Mockito.any(HeaderFrame.class));
        Mockito.verify(context).fireRead(argument.capture());
        Mockito.verifyNoMoreInteractions(context);

        assertNotNull(argument.getValue());
        assertTrue(argument.getValue().getBody() instanceof Open);

        Open decoded = (Open) argument.getValue().getBody();

        assertTrue(decoded.hasContainerId());  // Defaults to empty string from proton-j
        assertFalse(decoded.hasHostname());
        assertFalse(decoded.hasMaxFrameSize());
        assertFalse(decoded.hasChannelMax());
        assertFalse(decoded.hasIdleTimeout());
        assertFalse(decoded.hasOutgoingLocales());
        assertFalse(decoded.hasIncomingLocales());
        assertFalse(decoded.hasOfferedCapabilites());
        assertFalse(decoded.hasDesiredCapabilites());
        assertFalse(decoded.hasProperties());
    }

    @Test
    public void testDecodeSimpleOpenEncodedFrame() throws Exception {
        // Frame data for: Open
        //   Open{ containerId='container', hostname='localhost', maxFrameSize=16384, channelMax=65535,
        //         idleTimeOut=30000, outgoingLocales=null, incomingLocales=null, offeredCapabilities=null,
        //         desiredCapabilities=null, properties=null}
        final byte[] basicOpen = new byte[] {0, 0, 0, 49, 2, 0, 0, 0, 0, 83, 16, -64, 36, 5, -95, 9, 99, 111,
                                             110, 116, 97, 105, 110, 101, 114, -95, 9, 108, 111, 99, 97, 108,
                                             104, 111, 115, 116, 112, 0, 0, 64, 0, 96, -1, -1, 112, 0, 0, 117, 48};
        ArgumentCaptor<ProtocolFrame> argument = ArgumentCaptor.forClass(ProtocolFrame.class);

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        EngineHandlerContext context = Mockito.mock(EngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());
        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(basicOpen));

        Mockito.verify(context).fireRead(Mockito.any(HeaderFrame.class));
        Mockito.verify(context).fireRead(argument.capture());
        Mockito.verifyNoMoreInteractions(context);

        assertNotNull(argument.getValue());
        assertTrue(argument.getValue().getBody() instanceof Open);

        Open decoded = (Open) argument.getValue().getBody();

        assertTrue(decoded.hasContainerId());
        assertEquals("container", decoded.getContainerId());
        assertTrue(decoded.hasHostname());
        assertEquals("localhost", decoded.getHostname());
        assertTrue(decoded.hasMaxFrameSize());
        assertEquals(16384, decoded.getMaxFrameSize());
        assertFalse(decoded.hasChannelMax());
        assertTrue(decoded.hasIdleTimeout());
        assertEquals(30000, decoded.getIdleTimeOut());
        assertFalse(decoded.hasOutgoingLocales());
        assertFalse(decoded.hasIncomingLocales());
        assertFalse(decoded.hasOfferedCapabilites());
        assertFalse(decoded.hasDesiredCapabilites());
        assertFalse(decoded.hasProperties());
    }

    /*
     * Test that empty frames, as used for heartbeating, decode as expected.
     */
    @Test
    public void testDecodeEmptyFrame() throws Exception {
        // http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#doc-idp124752
        // Description: '8byte sized' empty AMQP frame
        byte[] emptyFrame = new byte[] { (byte) 0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00 };

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        EngineHandlerContext context = Mockito.mock(EngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());

        Mockito.verify(context).fireRead(Mockito.any(HeaderFrame.class));
        Mockito.verifyNoMoreInteractions(context);

        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(emptyFrame));

        ArgumentCaptor<ProtocolFrame> argument = ArgumentCaptor.forClass(ProtocolFrame.class);
        Mockito.verify(context).fireRead(argument.capture());
        Mockito.verifyNoMoreInteractions(context);

        assertNotNull(argument.getValue());
        assertTrue(argument.getValue() instanceof EmptyFrame);
    }

    /*
     * Test that frames indicating they are under 8 bytes (the minimum size of the frame header) causes an error.
     */
    @Test
    public void testInputOfFrameWithInvalidSizeBelowMinimumPossible() throws Exception
    {
        // http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#doc-idp124752
        // Description: '7byte sized' AMQP frame header
        byte[] undersizedFrameHeader = new byte[] { (byte) 0x00, 0x00, 0x00, 0x07, 0x02, 0x00, 0x00, 0x00 };

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        EngineHandlerContext context = Mockito.mock(EngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());

        Mockito.verify(context).fireRead(Mockito.any(HeaderFrame.class));
        Mockito.verifyNoMoreInteractions(context);

        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(undersizedFrameHeader));

        ArgumentCaptor<Throwable> argument = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(context).fireDecodingError(argument.capture());
        Mockito.verifyNoMoreInteractions(context);

        Throwable t = argument.getValue();
        assertTrue("Unexpected exception type:" + t, t instanceof IOException);
        assertThat(t.getMessage(), containsString("frame size 7 smaller than minimum"));
    }

    /*
     * Test that frames indicating a DOFF under 8 bytes (the minimum size of the frame header) causes an error.
     */
    @Test
    public void testInputOfFrameWithInvalidDoffBelowMinimumPossible() throws Exception
    {
        // http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#doc-idp124752
        // Description: '8byte sized' AMQP frame header with invalid doff of 1[*4 = 4bytes]
        byte[] underMinDoffFrameHeader = new byte[] { (byte) 0x00, 0x00, 0x00, 0x08, 0x01, 0x00, 0x00, 0x00 };

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        EngineHandlerContext context = Mockito.mock(EngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());

        Mockito.verify(context).fireRead(Mockito.any(HeaderFrame.class));
        Mockito.verifyNoMoreInteractions(context);

        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(underMinDoffFrameHeader));

        ArgumentCaptor<Throwable> argument = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(context).fireDecodingError(argument.capture());
        Mockito.verifyNoMoreInteractions(context);

        Throwable t = argument.getValue();
        assertTrue("Unexpected exception type:" + t, t instanceof IOException);
        assertThat(t.getMessage(), containsString("data offset 4 smaller than minimum"));
    }

    /*
     * Test that frames indicating a DOFF larger than the frame size cause expected error.
     */
    @Test
    public void testInputOfFrameWithInvalidDoffAboveMaximumPossible() throws Exception
    {
        // http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#doc-idp124752
        // Description: '8byte sized' AMQP frame header with invalid doff of 3[*4 = 12bytes]
        byte[] overFrameSizeDoffFrameHeader = new byte[] { (byte) 0x00, 0x00, 0x00, 0x08, 0x03, 0x00, 0x00, 0x00 };

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        EngineHandlerContext context = Mockito.mock(EngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());

        Mockito.verify(context).fireRead(Mockito.any(HeaderFrame.class));
        Mockito.verifyNoMoreInteractions(context);

        handler.handleRead(context, ProtonByteBufferAllocator.DEFAULT.wrap(overFrameSizeDoffFrameHeader));

        ArgumentCaptor<Throwable> argument = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(context).fireDecodingError(argument.capture());
        Mockito.verifyNoMoreInteractions(context);

        Throwable t = argument.getValue();
        assertTrue("Unexpected exception type:" + t, t instanceof IOException);
        assertThat(t.getMessage(), containsString("data offset 12 larger than the frame size 8"));
    }

    private ProtonFrameDecodingHandler createFrameDecoder() throws Exception {
        ProtonEngineConfiguration configuration = Mockito.mock(ProtonEngineConfiguration.class);
        Mockito.when(configuration.getInboundMaxFrameSize()).thenReturn(Integer.valueOf(65535));
        ProtonEngine engine = Mockito.mock(ProtonEngine.class);
        Mockito.when(engine.configuration()).thenReturn(configuration);
        EngineHandlerContext context = Mockito.mock(EngineHandlerContext.class);
        Mockito.when(context.getEngine()).thenReturn(engine);

        ProtonFrameDecodingHandler handler = new ProtonFrameDecodingHandler();
        handler.handlerAdded(context);

        return handler;
    }

    private Engine createEngine() {
        ProtonEngine transport = new ProtonEngine();

        transport.pipeline().addLast("test", testHandler);
        transport.pipeline().addLast("frames", new ProtonFrameDecodingHandler());

        return transport;
    }
}
