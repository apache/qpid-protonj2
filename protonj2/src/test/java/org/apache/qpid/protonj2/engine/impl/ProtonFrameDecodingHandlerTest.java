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
package org.apache.qpid.protonj2.engine.impl;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.times;

import java.util.List;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.engine.EmptyEnvelope;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineHandlerContext;
import org.apache.qpid.protonj2.engine.HeaderEnvelope;
import org.apache.qpid.protonj2.engine.IncomingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.protonj2.engine.util.FrameReadSinkTransportHandler;
import org.apache.qpid.protonj2.engine.util.FrameRecordingTransportHandler;
import org.apache.qpid.protonj2.engine.util.FrameWriteSinkTransportHandler;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;
import org.apache.qpid.protonj2.types.transport.Open;
import org.apache.qpid.protonj2.types.transport.Transfer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class ProtonFrameDecodingHandlerTest {

    private FrameRecordingTransportHandler testHandler;

    @BeforeEach
    public void setUp() {
        testHandler = new FrameRecordingTransportHandler();
    }

    @Test
    public void testDecodeValidHeaderTriggersHeaderRead() {
        Engine engine = createEngine();

        engine.start();

        // Check for Header processing
        engine.pipeline().fireRead(AMQPHeader.getAMQPHeader().getBuffer());

        Object frame = testHandler.getFramesRead().get(0);
        assertTrue(frame instanceof HeaderEnvelope);
        HeaderEnvelope header = (HeaderEnvelope) frame;
        assertEquals(AMQPHeader.getAMQPHeader(), header.getBody());
    }

    @Test
    public void testReadValidHeaderInSingleByteChunks() throws Exception {
        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(new byte[] { 'A' }));
        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(new byte[] { 'M' }));
        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(new byte[] { 'Q' }));
        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(new byte[] { 'P' }));
        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(new byte[] { 0 }));
        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(new byte[] { 1 }));
        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(new byte[] { 0 }));
        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(new byte[] { 0 }));

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
        Mockito.verifyNoMoreInteractions(context);
    }

    @Test
    public void testReadValidHeaderInSplitChunks() throws Exception {
        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(new byte[] { 'A', 'M', 'Q', 'P' }));
        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(new byte[] { 0, 1, 0, 0 }));

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
        Mockito.verifyNoMoreInteractions(context);
    }

    @Test
    public void testReadOfSaslHeaderDoesNotDisableWritesMonitoring() throws Exception {
        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(new byte[] { 'A', 'M', 'Q', 'P' }));
        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(new byte[] { 3, 1, 0, 0 }));

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verifyNoMoreInteractions(context);
    }

    @Test
    public void testDecodeValidSaslHeaderTriggersHeaderRead() {
        Engine engine = createEngine();

        engine.start();

        // Check for Header processing
        engine.pipeline().fireRead(AMQPHeader.getSASLHeader().getBuffer());

        Object frame = testHandler.getFramesRead().get(0);
        assertTrue(frame instanceof HeaderEnvelope);
        HeaderEnvelope header = (HeaderEnvelope) frame;
        assertEquals(AMQPHeader.getSASLHeader(), header.getBody());
    }

    @Test
    public void testInvalidHeaderBytesTriggersError() {
        ProtonFrameDecodingHandler handler = createFrameDecoder();
        EngineHandlerContext context = Mockito.mock(EngineHandlerContext.class);

        try {
            handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(new byte[] { 'S' }));
            fail("Handler should throw error on invalid input");
        } catch (Throwable error) {
            // Expected
        }

        // Verify that the parser accepts no new input once in error state.
        Mockito.clearInvocations(context);
        try {
            handler.handleRead(context, AMQPHeader.getSASLHeader().getBuffer());
            fail("Handler should throw error on additional input");
        } catch (Throwable error) {
            // Expected
        }
    }

    @Test
    public void testDecodeEmptyOpenEncodedFrame() throws Exception {
        // Frame data for: Open
        //   Open{ containerId="", hostname='null', maxFrameSize=4294967295, channelMax=65535,
        //         idleTimeOut=null, outgoingLocales=null, incomingLocales=null, offeredCapabilities=null,
        //         desiredCapabilities=null, properties=null}
        final byte[] emptyOpen = new byte[] {0, 0, 0, 16, 2, 0, 0, 0, 0, 83, 16, -64, 3, 1, -95, 0};

        ArgumentCaptor<IncomingAMQPEnvelope> argument = ArgumentCaptor.forClass(IncomingAMQPEnvelope.class);

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());
        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(emptyOpen));

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
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
        assertFalse(decoded.hasOfferedCapabilities());
        assertFalse(decoded.hasDesiredCapabilities());
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
        ArgumentCaptor<IncomingAMQPEnvelope> argument = ArgumentCaptor.forClass(IncomingAMQPEnvelope.class);

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(basicOpen));

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
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
        assertTrue(decoded.hasChannelMax());
        assertTrue(decoded.hasIdleTimeout());
        assertEquals(30000, decoded.getIdleTimeout());
        assertFalse(decoded.hasOutgoingLocales());
        assertFalse(decoded.hasIncomingLocales());
        assertFalse(decoded.hasOfferedCapabilities());
        assertFalse(decoded.hasDesiredCapabilities());
        assertFalse(decoded.hasProperties());
    }

    @Test
    public void testDecodePipelinedHeaderAndOpenEncodedFrame() throws Exception {
        // Frame data for: Open
        //   Open{ containerId='container', hostname='localhost', maxFrameSize=16384, channelMax=65535,
        //         idleTimeOut=30000, outgoingLocales=null, incomingLocales=null, offeredCapabilities=null,
        //         desiredCapabilities=null, properties=null}
        final byte[] basicOpen = new byte[] {'A', 'M', 'Q', 'P', 0, 1, 0, 0, // HEADER
                                             0, 0, 0, 49, 2, 0, 0, 0, 0, 83, 16, -64, 36, 5, -95, 9, 99, 111,
                                             110, 116, 97, 105, 110, 101, 114, -95, 9, 108, 111, 99, 97, 108,
                                             104, 111, 115, 116, 112, 0, 0, 64, 0, 96, -1, -1, 112, 0, 0, 117, 48};
        ArgumentCaptor<IncomingAMQPEnvelope> argument = ArgumentCaptor.forClass(IncomingAMQPEnvelope.class);

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(basicOpen));

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
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
        assertTrue(decoded.hasChannelMax());
        assertTrue(decoded.hasIdleTimeout());
        assertEquals(30000, decoded.getIdleTimeout());
        assertFalse(decoded.hasOutgoingLocales());
        assertFalse(decoded.hasIncomingLocales());
        assertFalse(decoded.hasOfferedCapabilities());
        assertFalse(decoded.hasDesiredCapabilities());
        assertFalse(decoded.hasProperties());
    }

    @Test
    public void testDecodePipelinedHeaderAndOpenEncodedFrameSplitAcrossTwoReads() throws Exception {
        // Frame data for: Open
        //   Open{ containerId='container', hostname='localhost', maxFrameSize=16384, channelMax=65535,
        //         idleTimeOut=30000, outgoingLocales=null, incomingLocales=null, offeredCapabilities=null,
        //         desiredCapabilities=null, properties=null}
        final byte[] basicOpen1 = new byte[] {
                'A', 'M', 'Q', 'P', 0, 1, 0, 0, // HEADER
                0, 0, 0, 49, 2, 0, 0, 0, 0, 83, 16, -64, 36, 5, -95, 9, 99, 111 };
        final byte[] basicOpen2 = new byte[] {
                110, 116, 97, 105, 110, 101, 114, -95, 9, 108, 111, 99, 97, 108,
                104, 111, 115, 116, 112, 0, 0, 64, 0, 96, -1, -1, 112, 0, 0, 117, 48 };
        ArgumentCaptor<IncomingAMQPEnvelope> argument = ArgumentCaptor.forClass(IncomingAMQPEnvelope.class);

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        final ProtonBuffer buffer1 = ProtonBufferAllocator.defaultAllocator().copy(basicOpen1);
        final ProtonBuffer buffer2 = ProtonBufferAllocator.defaultAllocator().copy(basicOpen2);

        handler.handleRead(context, buffer1);
        handler.handleRead(context, buffer2);

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
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
        assertTrue(decoded.hasChannelMax());
        assertTrue(decoded.hasIdleTimeout());
        assertEquals(30000, decoded.getIdleTimeout());
        assertFalse(decoded.hasOutgoingLocales());
        assertFalse(decoded.hasIncomingLocales());
        assertFalse(decoded.hasOfferedCapabilities());
        assertFalse(decoded.hasDesiredCapabilities());
        assertFalse(decoded.hasProperties());
    }

    @Test
    public void testDecodePipelinedHeaderAndOpenEncodedFrameSizeSplitAcrossTwoReads() throws Exception {
        // Frame data for: Open
        //   Open{ containerId='container', hostname='localhost', maxFrameSize=16384, channelMax=65535,
        //         idleTimeOut=30000, outgoingLocales=null, incomingLocales=null, offeredCapabilities=null,
        //         desiredCapabilities=null, properties=null}
        final byte[] basicOpen1 = new byte[] {
                'A', 'M', 'Q', 'P', 0, 1, 0, 0, 0 }; // HEADER + first frame byte
        final byte[] basicOpen2 = new byte[] { 0 };
        final byte[] basicOpen3 = new byte[] { 0, 49 };
        final byte[] basicOpen4 = new byte[] {
                2, 0, 0, 0, 0, 83, 16, -64, 36, 5, -95, 9, 99, 111,
                110, 116, 97, 105, 110, 101, 114, -95, 9, 108, 111, 99, 97, 108,
                104, 111, 115, 116, 112, 0, 0, 64, 0, 96, -1, -1, 112, 0, 0, 117, 48 };
        ArgumentCaptor<IncomingAMQPEnvelope> argument = ArgumentCaptor.forClass(IncomingAMQPEnvelope.class);

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        final ProtonBuffer buffer1 = ProtonBufferAllocator.defaultAllocator().copy(basicOpen1);
        final ProtonBuffer buffer2 = ProtonBufferAllocator.defaultAllocator().copy(basicOpen2);
        final ProtonBuffer buffer3 = ProtonBufferAllocator.defaultAllocator().copy(basicOpen3);
        final ProtonBuffer buffer4 = ProtonBufferAllocator.defaultAllocator().copy(basicOpen4);

        handler.handleRead(context, buffer1);
        handler.handleRead(context, buffer2);
        handler.handleRead(context, buffer3);
        handler.handleRead(context, buffer4);

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
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
        assertTrue(decoded.hasChannelMax());
        assertTrue(decoded.hasIdleTimeout());
        assertEquals(30000, decoded.getIdleTimeout());
        assertFalse(decoded.hasOutgoingLocales());
        assertFalse(decoded.hasIncomingLocales());
        assertFalse(decoded.hasOfferedCapabilities());
        assertFalse(decoded.hasDesiredCapabilities());
        assertFalse(decoded.hasProperties());
    }

    @Test
    public void testDecodePipelinedHeaderAndOpenEncodedFrameSplitAcrossThreeReads() throws Exception {
        // Frame data for: Open
        //   Open{ containerId='container', hostname='localhost', maxFrameSize=16384, channelMax=65535,
        //         idleTimeOut=30000, outgoingLocales=null, incomingLocales=null, offeredCapabilities=null,
        //         desiredCapabilities=null, properties=null}
        final byte[] basicOpen1 = new byte[] {
                'A', 'M', 'Q', 'P', 0, 1, 0, 0, // HEADER
                0, 0, 0, 49, 2, 0, 0, 0, 0, 83, 16, -64, 36, 5, -95, 9, 99, 111 };
        final byte[] basicOpen2 = new byte[] {
                110, 116, 97, 105, 110, 101, 114, -95, 9, 108, 111, 99, 97, 108 };
        final byte[] basicOpen3 = new byte[] {
                104, 111, 115, 116, 112, 0, 0, 64, 0, 96, -1, -1, 112, 0, 0, 117, 48 };
        ArgumentCaptor<IncomingAMQPEnvelope> argument = ArgumentCaptor.forClass(IncomingAMQPEnvelope.class);

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        final ProtonBuffer buffer1 = ProtonBufferAllocator.defaultAllocator().copy(basicOpen1);
        final ProtonBuffer buffer2 = ProtonBufferAllocator.defaultAllocator().copy(basicOpen2);
        final ProtonBuffer buffer3 = ProtonBufferAllocator.defaultAllocator().copy(basicOpen3);

        handler.handleRead(context, buffer1);
        handler.handleRead(context, buffer2);
        handler.handleRead(context, buffer3);

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
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
        assertTrue(decoded.hasChannelMax());
        assertTrue(decoded.hasIdleTimeout());
        assertEquals(30000, decoded.getIdleTimeout());
        assertFalse(decoded.hasOutgoingLocales());
        assertFalse(decoded.hasIncomingLocales());
        assertFalse(decoded.hasOfferedCapabilities());
        assertFalse(decoded.hasDesiredCapabilities());
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
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
        Mockito.verifyNoMoreInteractions(context);

        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(emptyFrame));

        ArgumentCaptor<IncomingAMQPEnvelope> argument = ArgumentCaptor.forClass(IncomingAMQPEnvelope.class);
        Mockito.verify(context).fireRead(argument.capture());
        Mockito.verifyNoMoreInteractions(context);

        assertNotNull(argument.getValue());
        assertTrue(argument.getValue() instanceof EmptyEnvelope);
    }

    /*
     * Test that two empty frames, as used for heartbeating, decode as expected when arriving back to back.
     */
    @Test
    public void testDecodeMultipleEmptyFrames() throws Exception {
        // http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#doc-idp124752
        // Description: 2x '8byte sized' empty AMQP frames
        byte[] emptyFrames = new byte[] { (byte) 0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00,
                                          (byte) 0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00 };

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
        Mockito.verifyNoMoreInteractions(context);

        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(emptyFrames));

        ArgumentCaptor<IncomingAMQPEnvelope> argument = ArgumentCaptor.forClass(IncomingAMQPEnvelope.class);
        Mockito.verify(context, Mockito.times(2)).fireRead(argument.capture());

        List<IncomingAMQPEnvelope> frames = argument.getAllValues();
        assertNotNull(frames);
        assertEquals(2, frames.size());
        assertTrue(frames.get(0) instanceof EmptyEnvelope);
        assertTrue(frames.get(1) instanceof EmptyEnvelope);

        Mockito.verifyNoMoreInteractions(context);
    }

    /*
     * Test that frames indicating they are under 8 bytes (the minimum size of the frame header) causes an error.
     */
    @Test
    public void testInputOfFrameWithInvalidSizeBelowMinimumPossible() throws Exception {
        // http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#doc-idp124752
        // Description: '7byte sized' AMQP frame header
        byte[] undersizedFrameHeader = new byte[] { (byte) 0x00, 0x00, 0x00, 0x07, 0x02, 0x00, 0x00, 0x00 };

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
        Mockito.verifyNoMoreInteractions(context);

        try {
            handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(undersizedFrameHeader));
            fail("Should indicate protocol has been violated.");
        } catch (ProtocolViolationException pve) {
            // Expected
            assertThat(pve.getMessage(), containsString("frame size 7 smaller than minimum"));
        }

        Mockito.verifyNoMoreInteractions(context);
    }

    /*
     * Test that frames indicating a DOFF under 8 bytes (the minimum size of the frame header) causes an error.
     */
    @Test
    public void testInputOfFrameWithInvalidDoffBelowMinimumPossible() throws Exception {
        // http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#doc-idp124752
        // Description: '8byte sized' AMQP frame header with invalid doff of 1[*4 = 4bytes]
        byte[] underMinDoffFrameHeader = new byte[] { (byte) 0x00, 0x00, 0x00, 0x08, 0x01, 0x00, 0x00, 0x00 };

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
        Mockito.verifyNoMoreInteractions(context);

        try {
            handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(underMinDoffFrameHeader));
            fail("Should indicate protocol has been violated.");
        } catch (ProtocolViolationException pve) {
            // Expected
            assertThat(pve.getMessage(), containsString("data offset 4 smaller than minimum"));
        }

        Mockito.verifyNoMoreInteractions(context);
    }

    /*
     * Test that frames indicating a DOFF larger than the frame size cause expected error.
     */
    @Test
    public void testInputOfFrameWithInvalidDoffAboveMaximumPossible() throws Exception {
        // http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#doc-idp124752
        // Description: '8byte sized' AMQP frame header with invalid doff of 3[*4 = 12bytes]
        byte[] overFrameSizeDoffFrameHeader = new byte[] { (byte) 0x00, 0x00, 0x00, 0x08, 0x03, 0x00, 0x00, 0x00 };

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
        Mockito.verifyNoMoreInteractions(context);

        try {
            handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(overFrameSizeDoffFrameHeader));
            fail("Should indicate protocol has been violated.");
        } catch (ProtocolViolationException pve) {
            // Expected
            assertThat(pve.getMessage(), containsString("data offset 12 larger than the frame size 8"));
        }

        Mockito.verifyNoMoreInteractions(context);
    }

    /*
     * Test that frame size above limit triggers error before attempting to decode the frame
     */
    @Test
    public void testFrameSizeThatExceedsMaximumFrameSizeLimitTriggersError() throws Exception {
        byte[] overFrameSizeLimitFrameHeader = new byte[] { (byte) 0xA0, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00 };

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
        Mockito.verifyNoMoreInteractions(context);

        try {
            handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(overFrameSizeLimitFrameHeader));
            fail("Should indicate frame limit has been violated.");
        } catch (ProtocolViolationException pve) {
            // Expected 2684354560 frame size is to big
            assertThat(pve.getMessage(), containsString("2684354560"));
            assertThat(pve.getMessage(), containsString("larger than maximum frame size"));
        }

        Mockito.verifyNoMoreInteractions(context);
    }

    @Test
    public void testDecodeTransferFrameWithAttachedPayload() {
        // Frame data for: Transfer
        //   Transfer{handle=2, deliveryId=1, deliveryTag=\x00\x01, messageFormat=null, settled=true, more=false, rcvSettleMode=null, state=null, resume=false, aborted=false, batchable=false}
        //   payload of size: 169
        final byte[] completedTransfer = new byte[] {
            0, 0, 0, -63, 2, 0, 0, 0, 0, 83, 20, -64, 11, 5, 82, 2, 82, 1, -96, 2, 0, 1, 64, 65, 0, 83, 115,
            -48, 0, 0, 0, 28, 0, 0, 0, 3, -104, -107, -75, 19, 123, 103, 50, 77, 43, -73, 93, 29, 105, 64,
            -84, 45, 110, 64, -95, 4, 116, 101, 115, 116, 0, 83, 116, -63, 23, 2, -95, 9, 116, 105, 109, 101,
            115, 116, 97, 109, 112, -95, 9, 49, 50, 51, 52, 53, 54, 55, 56, 57, 0, 83, 117, -96, 100, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65};

        ArgumentCaptor<IncomingAMQPEnvelope> argument = ArgumentCaptor.forClass(IncomingAMQPEnvelope.class);

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());
        handler.handleRead(context, ProtonBufferAllocator.defaultAllocator().copy(completedTransfer));

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
        Mockito.verify(context).fireRead(argument.capture());
        Mockito.verifyNoMoreInteractions(context);

        assertNotNull(argument.getValue());
        assertTrue(argument.getValue().getBody() instanceof Transfer);
        assertNotNull(argument.getValue().getPayload());
        assertTrue(argument.getValue().getPayload().isReadable());
        assertEquals(169, argument.getValue().getPayload().getReadableBytes());

        Transfer decoded = (Transfer) argument.getValue().getBody();

        assertEquals(2, decoded.getHandle());
        assertEquals(1, decoded.getDeliveryId());
        assertArrayEquals(new byte[] { 0, 1 }, decoded.getDeliveryTag().tagBytes());
    }

    @Test
    public void testDecodeTransferFrameWithAttachedPayloadSplitAcrossBuffers() {
        // Frame data for: Transfer
        //   Transfer{handle=2, deliveryId=1, deliveryTag=\x00\x01, messageFormat=null, settled=true, more=false, rcvSettleMode=null, state=null, resume=false, aborted=false, batchable=false}
        //   payload of size: 169
        final byte[] completedTransfer1 = new byte[] {
            0, 0, 0, -63, 2, 0, 0, 0, 0, 83, 20, -64, 11, 5, 82, 2, 82, 1, -96, 2, 0, 1, 64, 65, 0, 83, 115,
            -48, 0, 0, 0, 28, 0, 0, 0, 3, -104, -107, -75, 19, 123, 103, 50, 77, 43, -73, 93, 29, 105, 64};
        final byte[] completedTransfer2 = new byte[] {
            -84, 45, 110, 64, -95, 4, 116, 101, 115, 116, 0, 83, 116, -63, 23, 2, -95, 9, 116, 105, 109, 101,
            115, 116, 97, 109, 112, -95, 9, 49, 50, 51, 52, 53, 54, 55, 56, 57, 0, 83, 117, -96, 100, 65, 65};
        final byte[] completedTransfer3 = new byte[] {
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65};

        ArgumentCaptor<IncomingAMQPEnvelope> argument = ArgumentCaptor.forClass(IncomingAMQPEnvelope.class);

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());

        final ProtonBuffer buffer1 = ProtonBufferAllocator.defaultAllocator().copy(completedTransfer1);
        final ProtonBuffer buffer2 = ProtonBufferAllocator.defaultAllocator().copy(completedTransfer2);
        final ProtonBuffer buffer3 = ProtonBufferAllocator.defaultAllocator().copy(completedTransfer3);

        handler.handleRead(context, buffer1);
        handler.handleRead(context, buffer2);
        handler.handleRead(context, buffer3);

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
        Mockito.verify(context).fireRead(argument.capture());
        Mockito.verifyNoMoreInteractions(context);

        assertNotNull(argument.getValue());
        assertTrue(argument.getValue().getBody() instanceof Transfer);
        assertNotNull(argument.getValue().getPayload());
        assertTrue(argument.getValue().getPayload().isReadable());
        assertEquals(169, argument.getValue().getPayload().getReadableBytes());

        Transfer decoded = (Transfer) argument.getValue().getBody();

        assertEquals(2, decoded.getHandle());
        assertEquals(1, decoded.getDeliveryId());
        assertArrayEquals(new byte[] { 0, 1 }, decoded.getDeliveryTag().tagBytes());
    }

    @Test
    public void testDecodeTransferFrameWithAttachedPayloadSplitAcrossBuffersAsContinuationOfPreviousProcessedRead() {
        // Frame data for: Transfer
        //   Transfer{handle=2, deliveryId=1, deliveryTag=\x00\x01, messageFormat=null, settled=true, more=false, rcvSettleMode=null, state=null, resume=false, aborted=false, batchable=false}
        //   payload of size: 169
        final byte[] completedTransfer1 = new byte[] {
            0, 0, 0, -63, 2, 0, 0, 0, 0, 83, 20, -64, 11, 5, 82, 2, 82, 1, -96, 2, 0, 1, 64, 65, 0, 83, 115,
            -48, 0, 0, 0, 28, 0, 0, 0, 3, -104, -107, -75, 19, 123, 103, 50, 77, 43, -73, 93, 29, 105, 64};
        final byte[] completedTransfer2 = new byte[] {
            -84, 45, 110, 64, -95, 4, 116, 101, 115, 116, 0, 83, 116, -63, 23, 2, -95, 9, 116, 105, 109, 101,
            115, 116, 97, 109, 112, -95, 9, 49, 50, 51, 52, 53, 54, 55, 56, 57, 0, 83, 117, -96, 100, 65, 65};
        final byte[] completedTransfer3 = new byte[] {
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65};

        ArgumentCaptor<IncomingAMQPEnvelope> argument = ArgumentCaptor.forClass(IncomingAMQPEnvelope.class);

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());

        final ProtonBuffer buffer1 = ProtonBufferAllocator.defaultAllocator().allocate(completedTransfer1.length + 100);
        buffer1.advanceWriteOffset(100);
        buffer1.advanceReadOffset(100);
        buffer1.writeBytes(completedTransfer1);

        final ProtonBuffer buffer2 = ProtonBufferAllocator.defaultAllocator().copy(completedTransfer2);
        final ProtonBuffer buffer3 = ProtonBufferAllocator.defaultAllocator().copy(completedTransfer3);

        handler.handleRead(context, buffer1);
        handler.handleRead(context, buffer2);
        handler.handleRead(context, buffer3);

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
        Mockito.verify(context).fireRead(argument.capture());
        Mockito.verifyNoMoreInteractions(context);

        assertNotNull(argument.getValue());
        assertTrue(argument.getValue().getBody() instanceof Transfer);
        assertNotNull(argument.getValue().getPayload());
        assertTrue(argument.getValue().getPayload().isReadable());
        assertEquals(169, argument.getValue().getPayload().getReadableBytes());

        Transfer decoded = (Transfer) argument.getValue().getBody();

        assertEquals(2, decoded.getHandle());
        assertEquals(1, decoded.getDeliveryId());
        assertArrayEquals(new byte[] { 0, 1 }, decoded.getDeliveryTag().tagBytes());
    }

    @Test
    public void testDecodeTransferFrameWithAttachedPayloadSplitAcrossBuffersAsContinuationOfPreviousProcessedReadAndAnotherFrameFollowing() {
        // Frame data for: Transfer
        //   Transfer{handle=2, deliveryId=1, deliveryTag=\x00\x01, messageFormat=null, settled=true, more=false, rcvSettleMode=null, state=null, resume=false, aborted=false, batchable=false}
        //   payload of size: 169
        final byte[] completedTransfer1 = new byte[] {
            0, 0, 0, -63, 2, 0, 0, 0, 0, 83, 20, -64, 11, 5, 82, 2, 82, 1, -96, 2, 0, 1, 64, 65, 0, 83, 115,
            -48, 0, 0, 0, 28, 0, 0, 0, 3, -104, -107, -75, 19, 123, 103, 50, 77, 43, -73, 93, 29, 105, 64};
        final byte[] completedTransfer2 = new byte[] {
            -84, 45, 110, 64, -95, 4, 116, 101, 115, 116, 0, 83, 116, -63, 23, 2, -95, 9, 116, 105, 109, 101,
            115, 116, 97, 109, 112, -95, 9, 49, 50, 51, 52, 53, 54, 55, 56, 57, 0, 83, 117, -96, 100, 65, 65};
        final byte[] completedTransfer3 = new byte[] {
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
            65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65};
        // Frame data for: Open
        //   Open{ containerId="", hostname='null', maxFrameSize=4294967295, channelMax=65535,
        //         idleTimeOut=null, outgoingLocales=null, incomingLocales=null, offeredCapabilities=null,
        //         desiredCapabilities=null, properties=null}
        final byte[] emptyOpen = new byte[] {0, 0, 0, 16, 2, 0, 0, 0, 0, 83, 16, -64, 3, 1, -95, 0};

        ArgumentCaptor<IncomingAMQPEnvelope> argument = ArgumentCaptor.forClass(IncomingAMQPEnvelope.class);

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());

        final ProtonBuffer buffer1 = ProtonBufferAllocator.defaultAllocator().allocate(completedTransfer1.length + 100);
        buffer1.advanceWriteOffset(100);
        buffer1.advanceReadOffset(100);
        buffer1.writeBytes(completedTransfer1);

        final ProtonBuffer buffer2 = ProtonBufferAllocator.defaultAllocator().copy(completedTransfer2);

        final ProtonBuffer buffer3 = ProtonBufferAllocator.defaultAllocator().allocate(completedTransfer3.length + emptyOpen.length);
        buffer3.writeBytes(completedTransfer3);
        buffer3.writeBytes(emptyOpen);

        handler.handleRead(context, buffer1);
        handler.handleRead(context, buffer2);
        handler.handleRead(context, buffer3);

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
        Mockito.verify(context, times(2)).fireRead(argument.capture());
        Mockito.verifyNoMoreInteractions(context);

        List<IncomingAMQPEnvelope> arguments = argument.getAllValues();

        assertNotNull(arguments.get(0));
        assertTrue(arguments.get(0).getBody() instanceof Transfer);
        assertNotNull(arguments.get(0).getPayload());
        assertTrue(arguments.get(0).getPayload().isReadable());
        assertEquals(169, arguments.get(0).getPayload().getReadableBytes());

        Transfer transfer = (Transfer) arguments.get(0).getBody();

        assertEquals(2, transfer.getHandle());
        assertEquals(1, transfer.getDeliveryId());
        assertArrayEquals(new byte[] { 0, 1 }, transfer.getDeliveryTag().tagBytes());

        assertNotNull(arguments.get(1));
        assertTrue(arguments.get(1).getBody() instanceof Open);

        Open open = (Open) arguments.get(1).getBody();

        assertTrue(open.hasContainerId());  // Defaults to empty string from proton-j
        assertFalse(open.hasHostname());
        assertFalse(open.hasMaxFrameSize());
        assertFalse(open.hasChannelMax());
        assertFalse(open.hasIdleTimeout());
        assertFalse(open.hasOutgoingLocales());
        assertFalse(open.hasIncomingLocales());
        assertFalse(open.hasOfferedCapabilities());
        assertFalse(open.hasDesiredCapabilities());
        assertFalse(open.hasProperties());
    }

    @Test
    public void testDecodeOfSplitFramedMessage() throws Exception {
        final String capture1 = "00 00 01 4c 02 00 00 00 00 53 14 c0 1d 0b 43 43 " +
                                "a0 10 cf 8c f4 f8 93 5d b6 47 a2 19 3f 87 34 6f " +
                                "03 97 43 40 42 40 40 40 40 41";
        final String capture2 = "00 53 70 c0 0b 05 40 40 70 48 19 08 00 40 52 06 " +
                                "00 53 71 c1 24 02 a3 10 78 2d 6f 70 74 2d 6c 6f " +
                                "63 6b 2d 74 6f 6b 65 6e 98 f8 f4 8c cf 5d 93 47 " +
                                "b6 a2 19 3f 87 34 6f 03 97 00 53 72 c1 9b 0a a3 " +
                                "13 78 2d 6f 70 74 2d 65 6e 71 75 65 75 65 64 2d " +
                                "74 69 6d 65 83 00 00 01 8e 42 cd 59 63 a3 15 78 " +
                                "2d 6f 70 74 2d 73 65 71 75 65 6e 63 65 2d 6e 75 " +
                                "6d 62 65 72 81 00 00 00 00 00 00 07 2d a3 12 78 " +
                                "2d 6f 70 74 2d 6c 6f 63 6b 65 64 2d 75 6e 74 69 " +
                                "6c 83 00 00 01 8e 43 db 33 f3 a3 1d 78 2d 6f 70 " +
                                "74 2d 65 6e 71 75 65 75 65 2d 73 65 71 75 65 6e " +
                                "63 65 2d 6e 75 6d 62 65 72 81 00 00 00 00 00 00 " +
                                "07 2d a3 13 78 2d 6f 70 74 2d 6d 65 73 73 61 67 " +
                                "65 2d 73 74 61 74 65 54 00 00 53 73 c0 3f 0d a1 " +
                                "20 39 34 39 37 36 34 61 61 38 30 37 62 34 30 37 " +
                                "38 39 39 64 32 35 66 61 65 61 63 65 36 61 38 34 " +
                                "65 40 40 40 40 40 40 40 83 00 00 01 8e 8a e6 61 " +
                                "63 83 00 00 01 8e 42 cd 59 63 40 40 40 00 53 75 " +
                                "a0 00";

        final byte[] packet1 = convertCaptureToByteArray(capture1);
        final byte[] packet2 = convertCaptureToByteArray(capture2);

        ArgumentCaptor<IncomingAMQPEnvelope> argument = ArgumentCaptor.forClass(IncomingAMQPEnvelope.class);

        ProtonFrameDecodingHandler handler = createFrameDecoder();
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);

        handler.handleRead(context, AMQPHeader.getAMQPHeader().getBuffer());

        final ProtonBuffer buffer1 = ProtonBufferAllocator.defaultAllocator().copy(packet1);
        final ProtonBuffer buffer2 = ProtonBufferAllocator.defaultAllocator().copy(packet2);

        handler.handleRead(context, buffer1);
        handler.handleRead(context, buffer2);

        Mockito.verify(context).fireRead(Mockito.any(HeaderEnvelope.class));
        Mockito.verify(context).interestMask(ProtonEngineHandlerContext.HANDLER_READS);
        Mockito.verify(context, times(1)).fireRead(argument.capture());
        Mockito.verifyNoMoreInteractions(context);

        List<IncomingAMQPEnvelope> arguments = argument.getAllValues();

        assertNotNull(arguments.get(0));
        assertTrue(arguments.get(0).getBody() instanceof Transfer);
        assertNotNull(arguments.get(0).getPayload());
        assertTrue(arguments.get(0).getPayload().isReadable());
        assertEquals(290, arguments.get(0).getPayload().getReadableBytes());

        ProtonBuffer buffer = arguments.get(0).getPayload();
        Decoder decoder = CodecFactory.getDefaultDecoder();
        DecoderState decoderState = decoder.getCachedDecoderState();

        final Header header = (Header) decoder.readObject(buffer, decoderState);
        final DeliveryAnnotations deliveryAnnotations = (DeliveryAnnotations) decoder.readObject(buffer, decoderState);
        final MessageAnnotations messageAnnotations = (MessageAnnotations) decoder.readObject(buffer, decoderState);
        final Properties properties = (Properties) decoder.readObject(buffer, decoderState);
        final Section<?> body = (Section<?>) decoder.readObject(buffer, decoderState);

        assertNotNull(header);
        assertNotNull(deliveryAnnotations);
        assertNotNull(messageAnnotations);
        assertNotNull(properties);
        assertNotNull(body);
    }

    private byte[] convertCaptureToByteArray(String capture) throws Exception {
        final String[] hexStrings = capture.split(" ");
        final byte[] capturedBytes = new byte[hexStrings.length];

        for(int i = 0; i < hexStrings.length; ++i) {
            capturedBytes[i] = (byte) Integer.parseUnsignedInt(hexStrings[i], 16);
        }

        return capturedBytes;
    }

    private ProtonFrameDecodingHandler createFrameDecoder() {
        ProtonEngineConfiguration configuration = Mockito.mock(ProtonEngineConfiguration.class);
        Mockito.when(configuration.getInboundMaxFrameSize()).thenReturn(Long.valueOf(65535));
        Mockito.when(configuration.getOutboundMaxFrameSize()).thenReturn(Long.valueOf(65535));
        Mockito.when(configuration.getBufferAllocator()).thenReturn(ProtonBufferAllocator.defaultAllocator());
        ProtonEngine engine = Mockito.mock(ProtonEngine.class);
        Mockito.when(engine.configuration()).thenReturn(configuration);
        Mockito.when(engine.isWritable()).thenReturn(Boolean.TRUE);
        ProtonEngineHandlerContext context = Mockito.mock(ProtonEngineHandlerContext.class);
        Mockito.when(context.engine()).thenReturn(engine);

        ProtonFrameDecodingHandler handler = new ProtonFrameDecodingHandler();
        handler.handlerAdded(context);

        return handler;
    }

    private Engine createEngine() {
        ProtonEngine engine = new ProtonEngine();

        engine.pipeline().addLast("read-sink", new FrameReadSinkTransportHandler());
        engine.pipeline().addLast("test", testHandler);
        engine.pipeline().addLast("frames", new ProtonFrameDecodingHandler());
        engine.pipeline().addLast("write-sink", new FrameWriteSinkTransportHandler());

        return engine;
    }
}
