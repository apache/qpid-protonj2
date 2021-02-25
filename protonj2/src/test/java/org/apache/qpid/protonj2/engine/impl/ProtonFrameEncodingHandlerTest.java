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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.engine.EngineHandlerContext;
import org.apache.qpid.protonj2.engine.OutgoingProtocolFrame;
import org.apache.qpid.protonj2.engine.ProtocolFramePool;
import org.apache.qpid.protonj2.types.transport.Transfer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class ProtonFrameEncodingHandlerTest {

    private static final int FRAME_DOFF_SIZE = 2;
    private static final byte AMQP_FRAME_TYPE = (byte) 0;

    private ProtocolFramePool<OutgoingProtocolFrame> framePool;

    private ProtonEngineConfiguration configuration;
    private ProtonEngine engine;
    private EngineHandlerContext context;

    private final Random random = new Random();
    private final long randomSeed = System.currentTimeMillis();

    @BeforeEach
    void setUp() {
        random.setSeed(randomSeed);

        framePool = ProtocolFramePool.outgoingFramePool();

        configuration = Mockito.mock(ProtonEngineConfiguration.class);
        Mockito.when(configuration.getInboundMaxFrameSize()).thenReturn(Integer.valueOf(65535));
        Mockito.when(configuration.getOutboundMaxFrameSize()).thenReturn(Integer.valueOf(65535));
        Mockito.when(configuration.getBufferAllocator()).thenReturn(ProtonByteBufferAllocator.DEFAULT);

        engine = Mockito.mock(ProtonEngine.class);
        Mockito.when(engine.configuration()).thenReturn(configuration);
        Mockito.when(engine.isWritable()).thenReturn(Boolean.TRUE);

        context = Mockito.mock(EngineHandlerContext.class);
        Mockito.when(context.engine()).thenReturn(engine);
    }

    @Test
    void testEncodeBasicTransfer() {
        ProtonFrameEncodingHandler handler = new ProtonFrameEncodingHandler();
        handler.handlerAdded(context);

        Transfer transfer = new Transfer();
        transfer.setHandle(0);
        transfer.setDeliveryId(0);
        transfer.setDeliveryTag(new byte[] {0});

        OutgoingProtocolFrame frame = framePool.take(transfer, 32, null);

        handler.handleWrite(context, frame);

        ArgumentCaptor<ProtonBuffer> argument = ArgumentCaptor.forClass(ProtonBuffer.class);
        Mockito.verify(context).fireWrite(argument.capture());

        ProtonBuffer output = argument.getValue();

        assertNotNull(output);
        assertTrue(output.getWriteIndex() > 0);

        final int bufferSize = output.getReadableBytes();

        assertEquals(bufferSize, output.readInt());
        assertEquals(FRAME_DOFF_SIZE, output.readByte());
        assertEquals(AMQP_FRAME_TYPE, output.readByte());
        assertEquals(32, output.readShort());

        final Transfer decodedTransfer = decode(output);
        assertEquals(transfer.getHandle(), decodedTransfer.getHandle());
        assertEquals(transfer.getDeliveryId(), decodedTransfer.getDeliveryId());
        assertEquals(transfer.getDeliveryTag(), decodedTransfer.getDeliveryTag());
        assertEquals(transfer.getMore(), decodedTransfer.getMore());
    }

    @Test
    void testEncodeBasicTransferWthPayloadThatFitsIntoFrame() {
        ProtonFrameEncodingHandler handler = new ProtonFrameEncodingHandler();
        handler.handlerAdded(context);

        Transfer transfer = new Transfer();
        transfer.setHandle(0);
        transfer.setDeliveryId(0);
        transfer.setDeliveryTag(new byte[] {0});

        final byte[] payload = new byte[64];

        random.nextBytes(payload);

        OutgoingProtocolFrame frame = framePool.take(transfer, 32, ProtonByteBufferAllocator.DEFAULT.wrap(payload));

        handler.handleWrite(context, frame);

        ArgumentCaptor<ProtonBuffer> argument = ArgumentCaptor.forClass(ProtonBuffer.class);
        Mockito.verify(context).fireWrite(argument.capture());

        ProtonBuffer output = argument.getValue();

        assertNotNull(output);
        assertTrue(output.getWriteIndex() > 0);

        final int bufferSize = output.getReadableBytes();

        assertEquals(bufferSize, output.readInt());
        assertEquals(FRAME_DOFF_SIZE, output.readByte());
        assertEquals(AMQP_FRAME_TYPE, output.readByte());
        assertEquals(32, output.readShort());

        final Transfer decodedTransfer = decode(output);
        assertEquals(transfer.getHandle(), decodedTransfer.getHandle());
        assertEquals(transfer.getDeliveryId(), decodedTransfer.getDeliveryId());
        assertEquals(transfer.getDeliveryTag(), decodedTransfer.getDeliveryTag());
        assertEquals(transfer.getMore(), decodedTransfer.getMore());
    }

    @Test
    void testEncodeBasicTransferWthPayloadThatDoesNotFitIntoFrame() {
        ProtonFrameEncodingHandler handler = new ProtonFrameEncodingHandler();
        handler.handlerAdded(context);

        Transfer transfer = new Transfer();
        transfer.setHandle(0);
        transfer.setDeliveryId(0);
        transfer.setDeliveryTag(new byte[] {0});

        final byte[] payload = new byte[configuration.getOutboundMaxFrameSize() * 2];
        final AtomicBoolean toLargeHandlerCalled = new AtomicBoolean();

        random.nextBytes(payload);

        OutgoingProtocolFrame frame = framePool.take(transfer, 32, ProtonByteBufferAllocator.DEFAULT.wrap(payload));
        frame.setPayloadToLargeHandler((performative) -> {
            transfer.setMore(true);
            toLargeHandlerCalled.set(true);
        });

        handler.handleWrite(context, frame);

        ArgumentCaptor<ProtonBuffer> argument = ArgumentCaptor.forClass(ProtonBuffer.class);
        Mockito.verify(context).fireWrite(argument.capture());

        ProtonBuffer output = argument.getValue();

        assertTrue(toLargeHandlerCalled.get());
        assertNotNull(output);
        assertEquals(output.getReadableBytes(), configuration.getOutboundMaxFrameSize());

        final int bufferSize = output.getReadableBytes();

        assertEquals(configuration.getOutboundMaxFrameSize(), output.maxCapacity());
        assertEquals(bufferSize, output.readInt());
        assertEquals(FRAME_DOFF_SIZE, output.readByte());
        assertEquals(AMQP_FRAME_TYPE, output.readByte());
        assertEquals(32, output.readShort());

        final Transfer decodedTransfer = decode(output);
        assertEquals(transfer.getHandle(), decodedTransfer.getHandle());
        assertEquals(transfer.getDeliveryId(), decodedTransfer.getDeliveryId());
        assertEquals(transfer.getDeliveryTag(), decodedTransfer.getDeliveryTag());
        assertEquals(transfer.getMore(), decodedTransfer.getMore());
    }

    private Transfer decode(ProtonBuffer encoded) {
        Decoder decoder = CodecFactory.getDecoder();
        DecoderState decoderState = decoder.newDecoderState();

        Object decoded = decoder.readObject(encoded, decoderState);
        assertTrue(decoded instanceof Transfer);

        return (Transfer) decoded;
    }
}
