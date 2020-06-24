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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.engine.exceptions.FrameDecodingException;
import org.apache.qpid.protonj2.test.driver.ProtonTestPeer;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.junit.Test;

public class ProtonDecodeErrorTest extends ProtonEngineTestSupport {

    @Test(timeout = 10_000)
    public void testEmptyContainerIdInOpenProvokesDecodeError() throws Exception {
        // Provide the bytes for Open, but omit the mandatory container-id to provoke a decode error.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x0F, // Frame size = 15 bytes.
                                     0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                                     0x00, 0x53, 0x10, (byte) 0xC0, // Described-type, ulong type, open descriptor, list0.
                                     0x03, 0x01, 0x40 }; // size (3), count (1), container-id (null).

        doInvalidOpenProvokesDecodeErrorTestImpl(bytes, "The container-id field cannot be omitted");
    }

    @Test(timeout = 10_000)
    public void testEmptyOpenProvokesDecodeError() throws Exception {
        // Provide the bytes for Open, but omit the mandatory container-id to provoke a decode error.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x0C, // Frame size = 12 bytes.
                                     0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                                     0x00, 0x53, 0x10, 0x45};// Described-type, ulong type, open descriptor, list0.

        doInvalidOpenProvokesDecodeErrorTestImpl(bytes, "The container-id field cannot be omitted");
    }
    private void doInvalidOpenProvokesDecodeErrorTestImpl(byte[] bytes, String errorDescription) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen();
        peer.remoteBytes().withBytes(bytes).queue();
        peer.expectClose().withError(AmqpError.DECODE_ERROR.toString(), errorDescription);

        engine.start().open();

        peer.waitForScriptToCompleteIgnoreErrors();

        assertNotNull(failure);
        assertTrue(failure instanceof FrameDecodingException);
        assertEquals(errorDescription, failure.getMessage());
    }

    @Test(timeout = 10_000)
    public void testEmptyBeginProvokesDecodeError() throws Exception {
        // Provide the bytes for Begin, but omit any fields to provoke a decode error.
        byte[] bytes = new byte[] {
            0x00, 0x00, 0x00, 0x0C, // Frame size = 12 bytes.
            0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
            0x00, 0x53, 0x11, 0x45};// Described-type, ulong type, Begin descriptor, list0.

        doInvalidBeginProvokesDecodeErrorTestImpl(bytes, "The next-outgoing-id field cannot be omitted");
    }

    @Test(timeout = 10_000)
    public void testTruncatedBeginProvokesDecodeError1() throws Exception {
        // Provide the bytes for Begin, but only give a null (i-e not-present) for the remote-channel.
        byte[] bytes = new byte[] {
            0x00, 0x00, 0x00, 0x0F, // Frame size = 15 bytes.
            0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
            0x00, 0x53, 0x11, (byte) 0xC0, // Described-type, ulong type, Begin descriptor, list8.
            0x03, 0x01, 0x40 }; // size (3), count (1), remote-channel (null).

        doInvalidBeginProvokesDecodeErrorTestImpl(bytes, "The next-outgoing-id field cannot be omitted");
    }

    @Test(timeout = 10_000)
    public void testTruncatedBeginProvokesDecodeError2() throws Exception {
        // Provide the bytes for Begin, but only give a [not-present remote-channel +] next-outgoing-id and incoming-window. Provokes a decode error as there must be 4 fields.
        byte[] bytes = new byte[] {
            0x00, 0x00, 0x00, 0x11, // Frame size = 17 bytes.
            0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
            0x00, 0x53, 0x11, (byte) 0xC0, // Described-type, ulong type, Begin descriptor, list8.
            0x05, 0x03, 0x40, 0x43, 0x43 }; // size (5), count (3), remote-channel (null), next-outgoing-id (uint0), incoming-window (uint0).

        doInvalidBeginProvokesDecodeErrorTestImpl(bytes, "The outgoing-window field cannot be omitted");
    }

    @Test(timeout = 10_000)
    public void testTruncatedBeginProvokesDecodeError3() throws Exception {
        // Provide the bytes for Begin, but only give a [not-present remote-channel +] next-outgoing-id and incoming-window, and send not-present/null for outgoing-window. Provokes a decode error as must be present.
        byte[] bytes = new byte[] {
            0x00, 0x00, 0x00, 0x12, // Frame size = 18 bytes.
            0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
            0x00, 0x53, 0x11, (byte) 0xC0, // Described-type, ulong type, Begin descriptor, list8.
            0x06, 0x04, 0x40, 0x43, 0x43, 0x40 }; // size (5), count (4), remote-channel (null), next-outgoing-id (uint0), incoming-window (uint0), outgoing-window (null).

        doInvalidBeginProvokesDecodeErrorTestImpl(bytes, "The outgoing-window field cannot be omitted");
    }

    private void doInvalidBeginProvokesDecodeErrorTestImpl(byte[] bytes, String errorDescription) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.remoteBytes().withBytes(bytes).queue();
        peer.expectClose().withError(AmqpError.DECODE_ERROR.toString(), errorDescription);

        engine.start().open();

        peer.waitForScriptToCompleteIgnoreErrors();

        assertNotNull(failure);
        assertTrue(failure instanceof FrameDecodingException);
        assertEquals(errorDescription, failure.getMessage());
    }

    @Test(timeout = 10_000)
    public void testEmptyFlowProvokesDecodeError() throws Exception {
        // Provide the bytes for Flow, but omit any fields to provoke a decode error.
        byte[] bytes = new byte[] {
            0x00, 0x00, 0x00, 0x0C, // Frame size = 12 bytes.
            0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
            0x00, 0x53, 0x13, 0x45};// Described-type, ulong type, Flow descriptor, list0.

        doInvalidFlowProvokesDecodeErrorTestImpl(bytes, "The incoming-window field cannot be omitted");
    }

    @Test(timeout = 10_000)
    public void testTruncatedFlowProvokesDecodeError1() throws Exception {
        // Provide the bytes for Flow, but only give a 0 for the next-incoming-id. Provokes a decode error as there must be 4 fields.
        byte[] bytes = new byte[] {
            0x00, 0x00, 0x00, 0x0F, // Frame size = 15 bytes.
            0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
            0x00, 0x53, 0x13, (byte) 0xC0, // Described-type, ulong type, Flow descriptor, list8.
            0x03, 0x01, 0x43 }; // size (3), count (1), next-incoming-id (uint0).

        doInvalidFlowProvokesDecodeErrorTestImpl(bytes, "The incoming-window field cannot be omitted");
    }

    @Test(timeout = 10_000)
    public void testTruncatedFlowProvokesDecodeError2() throws Exception {
        // Provide the bytes for Flow, but only give a next-incoming-id and incoming-window and next-outgoing-id. Provokes a decode error as there must be 4 fields.
        byte[] bytes = new byte[] {
            0x00, 0x00, 0x00, 0x11, // Frame size = 17 bytes.
            0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
            0x00, 0x53, 0x13, (byte) 0xC0, // Described-type, ulong type, Flow descriptor, list8.
            0x05, 0x03, 0x43, 0x43, 0x43 }; // size (5), count (3), next-incoming-id (0), incoming-window (uint0), next-outgoing-id (uint0).

        doInvalidFlowProvokesDecodeErrorTestImpl(bytes, "The outgoing-window field cannot be omitted");
    }

    @Test(timeout = 10_000)
    public void testTruncatedFlowProvokesDecodeError3() throws Exception {
        // Provide the bytes for Flow, but only give a next-incoming-id and incoming-window and next-outgoing-id, and send not-present/null for outgoing-window. Provokes a decode error as must be present.
        byte[] bytes = new byte[] {
            0x00, 0x00, 0x00, 0x12, // Frame size = 18 bytes.
            0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
            0x00, 0x53, 0x13, (byte) 0xC0, // Described-type, ulong type, Flow descriptor, list8.
            0x06, 0x04, 0x43, 0x43, 0x43, 0x40 }; // size (5), count (4), next-incoming-id (0), incoming-window (uint0), next-outgoing-id (uint0), outgoing-window (null).

        doInvalidFlowProvokesDecodeErrorTestImpl(bytes, "The outgoing-window field cannot be omitted");
    }

    private void doInvalidFlowProvokesDecodeErrorTestImpl(byte[] bytes, String errorDescription) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.remoteBytes().withBytes(bytes).queue();  // Queue the frame for write after expected setup
        peer.expectClose().withError(AmqpError.DECODE_ERROR.toString(), errorDescription);

        Connection connection = engine.start().open();
        connection.session().open();

        peer.waitForScriptToCompleteIgnoreErrors();

        assertNotNull(failure);
        assertTrue(failure instanceof FrameDecodingException);
        assertEquals(errorDescription, failure.getMessage());
    }

    @Test(timeout = 10_000)
    public void testEmptyTransferProvokesDecodeError() throws Exception {
        // Provide the bytes for Transfer, but omit any fields to provoke a decode error.
        byte[] bytes = new byte[] {
            0x00, 0x00, 0x00, 0x0C, // Frame size = 12 bytes.
            0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
            0x00, 0x53, 0x14, 0x45};// Described-type, ulong type, Transfer descriptor, list0.

        doInvalidTransferProvokesDecodeErrorTestImpl(bytes, "The handle field cannot be omitted");
    }

    @Test(timeout = 10_000)
    public void testTruncatedTransferProvokesDecodeError() throws Exception {
        // Provide the bytes for Transfer, but only give a null for the not-present handle. Provokes a decode error as there must be a handle.
        byte[] bytes = new byte[] {
            0x00, 0x00, 0x00, 0x0F, // Frame size = 15 bytes.
            0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
            0x00, 0x53, 0x14, (byte) 0xC0, // Described-type, ulong type, Transfer descriptor, list8.
            0x03, 0x01, 0x40 }; // size (3), count (1), handle (null / not-present).

        doInvalidTransferProvokesDecodeErrorTestImpl(bytes, "The handle field cannot be omitted");
    }

    private void doInvalidTransferProvokesDecodeErrorTestImpl(byte[] bytes, String errorDescription) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.expectFlow().withLinkCredit(1);
        peer.remoteBytes().withBytes(bytes).queue();  // Queue the frame for write after expected setup
        peer.expectClose().withError(AmqpError.DECODE_ERROR.toString(), errorDescription);

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Receiver receiver = session.receiver("test").open();

        receiver.addCredit(1);

        peer.waitForScriptToCompleteIgnoreErrors();

        assertNotNull(failure);
        assertTrue(failure instanceof FrameDecodingException);
        assertEquals(errorDescription, failure.getMessage());
    }

    @Test(timeout = 10_000)
    public void testEmptyDispositionProvokesDecodeError() throws Exception {
        // Provide the bytes for Disposition, but omit any fields to provoke a decode error.
        byte[] bytes = new byte[] {
            0x00, 0x00, 0x00, 0x0C, // Frame size = 12 bytes.
            0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
            0x00, 0x53, 0x15, 0x45};// Described-type, ulong type, Disposition descriptor, list0.

        doInvalidDispositionProvokesDecodeErrorTestImpl(bytes, "The role field cannot be omitted");
    }

    @Test(timeout = 10_000)
    public void testTruncatedDispositionProvokesDecodeError() throws Exception {
        // Provide the bytes for Disposition, but only give a null/not-present for the 'first' field. Provokes a decode error as there must be a role and 'first'.
        byte[] bytes = new byte[] {
            0x00, 0x00, 0x00, 0x10, // Frame size = 16 bytes.
            0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
            0x00, 0x53, 0x15, (byte) 0xC0, // Described-type, ulong type, Disposition descriptor, list8.
            0x04, 0x02, 0x41, 0x40 }; // size (4), count (2), role (receiver - the peers perspective), first ( null / not-present)

        doInvalidDispositionProvokesDecodeErrorTestImpl(bytes, "The first field cannot be omitted");
    }

    private void doInvalidDispositionProvokesDecodeErrorTestImpl(byte[] bytes, String errorDescription) throws Exception {
        Engine engine = EngineFactory.PROTON.createNonSaslEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = createTestPeer(engine);

        ProtonBuffer payload = ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0, 1, 2, 3, 4});

        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectBegin().respond();
        peer.expectAttach().respond();
        peer.remoteFlow().withLinkCredit(10).queue();
        peer.expectTransfer();
        peer.remoteBytes().withBytes(bytes).queue();  // Queue the frame for write after expected setup
        peer.expectClose().withError(AmqpError.DECODE_ERROR.toString(), errorDescription);

        Connection connection = engine.start().open();
        Session session = connection.session().open();
        Sender sender = session.sender("test").open();

        OutgoingDelivery delivery = sender.next();
        delivery.writeBytes(payload.duplicate());

        peer.waitForScriptToCompleteIgnoreErrors();

        assertNotNull(failure);
        assertTrue(failure instanceof FrameDecodingException);
        assertEquals(errorDescription, failure.getMessage());
    }
}
