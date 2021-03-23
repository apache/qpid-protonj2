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
package org.apache.qpid.protonj2.engine.impl.sasl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.AMQPPerformativeEnvelopePool;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineSaslDriver.SaslState;
import org.apache.qpid.protonj2.engine.EngineState;
import org.apache.qpid.protonj2.engine.HeaderEnvelope;
import org.apache.qpid.protonj2.engine.PerformativeEnvelope;
import org.apache.qpid.protonj2.engine.SASLEnvelope;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.protonj2.engine.impl.ProtonConstants;
import org.apache.qpid.protonj2.engine.impl.ProtonEngine;
import org.apache.qpid.protonj2.engine.sasl.SaslOutcome;
import org.apache.qpid.protonj2.engine.sasl.SaslServerContext;
import org.apache.qpid.protonj2.engine.sasl.SaslServerListener;
import org.apache.qpid.protonj2.engine.util.FrameReadSinkTransportHandler;
import org.apache.qpid.protonj2.engine.util.FrameRecordingTransportHandler;
import org.apache.qpid.protonj2.engine.util.FrameWriteSinkTransportHandler;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.security.SaslCode;
import org.apache.qpid.protonj2.types.security.SaslInit;
import org.apache.qpid.protonj2.types.security.SaslMechanisms;
import org.apache.qpid.protonj2.types.security.SaslPerformative;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;
import org.apache.qpid.protonj2.types.transport.Open;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests SASL Handling by the SaslHandler TransportHandler class.
 */
public class ProtonSaslHandlerTest {

    private FrameRecordingTransportHandler testHandler;

    @BeforeEach
    public void setUp() {
        testHandler = new FrameRecordingTransportHandler();
    }

    @Test
    public void testCanRemoveSaslClientHandlerBeforeEngineStarted() {
        doTestCanRemoveSaslHandlerBeforeEngineStarted(false);
    }

    @Test
    public void testCanRemoveSaslServerHandlerBeforeEngineStarted() {
        doTestCanRemoveSaslHandlerBeforeEngineStarted(true);
    }

    private void doTestCanRemoveSaslHandlerBeforeEngineStarted(boolean server) {
        final Engine engine;

        if (server) {
            engine = createSaslServerEngine();
        } else {
            engine = createSaslClientEngine();
        }

        assertNotNull(engine.pipeline().find(ProtonConstants.SASL_PERFORMATIVE_HANDLER));

        engine.pipeline().remove(ProtonConstants.SASL_PERFORMATIVE_HANDLER);

        assertNull(engine.pipeline().find(ProtonConstants.SASL_PERFORMATIVE_HANDLER));
    }

    @Test
    public void testCannotInitiateSaslClientHandlerAfterEngineShutdown() {
        doTestCannotInitiateSaslHandlerAfterEngineShutdown(false);
    }

    @Test
    public void testCannotInitiateSaslServerHandlerAfterEngineShutdown() {
        doTestCannotInitiateSaslHandlerAfterEngineShutdown(true);
    }

    private void doTestCannotInitiateSaslHandlerAfterEngineShutdown(boolean server) {
        final Engine engine = createSaslCapableEngine();

        engine.shutdown();

        if (server) {
            assertThrows(IllegalStateException.class, ()-> engine.saslDriver().server());
        } else {
            assertThrows(IllegalStateException.class, ()-> engine.saslDriver().client());
        }
    }

    // TODO: Prevent removal from the pipeline.

    @Disabled("Need a mechanism to ensure handler is locked into pipeline")
    @Test
    public void testCannotRemoveSaslClientHandlerAfterEngineStarted() {
        doTestCanRemoveSaslHandlerAfterEngineStarted(false);
    }

    @Disabled("Need a mechanism to ensure handler is locked into pipeline")
    @Test
    public void testCannotRemoveSaslServerHandlerAfterEngineStarted() {
        doTestCanRemoveSaslHandlerAfterEngineStarted(true);
    }

    private void doTestCanRemoveSaslHandlerAfterEngineStarted(boolean server) {
        final Engine engine;

        if (server) {
            engine = createSaslServerEngine();
        } else {
            engine = createSaslClientEngine();
        }

        assertNotNull(engine.pipeline().find(ProtonConstants.SASL_PERFORMATIVE_HANDLER));

        engine.start();
        engine.pipeline().remove(ProtonConstants.SASL_PERFORMATIVE_HANDLER);

        assertNotNull(engine.pipeline().find(ProtonConstants.SASL_PERFORMATIVE_HANDLER));
    }

    @Test
    public void testCannotSaslDriverChangeMaxFrameSizeAfterSASLAuthBegins() {
        final Engine engine = createSaslServerEngine();

        engine.start();
        engine.pipeline().fireRead(new HeaderEnvelope(AMQPHeader.getSASLHeader()));

        assertThrows(IllegalStateException.class, () -> engine.saslDriver().setMaxFrameSize(1024));
    }

    @Test
    public void testCannotSaslDriverChangeMaxFrameSizeSmallerThanSpecMin() {
        final Engine engine = createSaslServerEngine();

        engine.start();

        assertThrows(IllegalArgumentException.class, () -> engine.saslDriver().setMaxFrameSize(256));
    }

    @Test
    public void testCanChangeSaslDriverMaxFrameSizeSmallerThanSpecMin() {
        final Engine engine = createSaslServerEngine();

        engine.start();
        engine.saslDriver().setMaxFrameSize(2048);

        assertEquals(2048, engine.saslDriver().getMaxFrameSize());
    }

    /**
     * Test that when the SASL server handler reads an AMQP Header before negotiations
     * have started it rejects the exchange by sending a SASL Header back to the remote
     */
    @Test
    public void testSaslRejectsAMQPHeader() {
        final AtomicBoolean headerRead = new AtomicBoolean();

        Engine engine = createSaslServerEngine();

        engine.saslDriver().server().setListener(new SaslServerListener() {

            @Override
            public void handleSaslResponse(SaslServerContext context, ProtonBuffer response) {
            }

            @Override
            public void handleSaslInit(SaslServerContext context, Symbol mechanism, ProtonBuffer initResponse) {
            }

            @Override
            public void handleSaslHeader(SaslServerContext context, AMQPHeader header) {
                headerRead.set(true);
            }
        });

        engine.start();

        try {
            engine.pipeline().fireRead(new HeaderEnvelope(AMQPHeader.getAMQPHeader()));
            fail("SASL handler should reject a non-SASL AMQP Header read.");
        } catch (ProtocolViolationException pve) {
            // Expected
        }

        assertFalse(headerRead.get(), "Should not receive a Header");

        List<PerformativeEnvelope<?>> frames = testHandler.getFramesWritten();

        assertEquals(1, frames.size(), "Sasl Anonymous exchange output not as expected");

        for (int i = 0; i < frames.size(); ++i) {
            PerformativeEnvelope<?> frame = frames.get(i);
            switch (i) {
                case 0:
                    assertTrue(frame.getFrameType() == HeaderEnvelope.HEADER_FRAME_TYPE);
                    HeaderEnvelope header = (HeaderEnvelope) frame;
                    assertTrue(header.getBody().isSaslHeader(), "Should have written a SASL Header in response");
                    break;
                default:
                    fail("Invalid Frame read during exchange: " + frame);
            }
        }

        assertEquals(EngineState.FAILED, engine.state());
    }

    @Test
    public void testExchangeSaslHeader() {
        final AtomicBoolean saslHeaderRead = new AtomicBoolean();

        Engine engine = createSaslServerEngine().start().getEngine();

        engine.saslDriver().server().setListener(new SaslServerListener() {

            @Override
            public void handleSaslHeader(SaslServerContext context, AMQPHeader header) {
                if (header.isSaslHeader()) {
                    saslHeaderRead.set(true);
                }
            }

            @Override
            public void handleSaslInit(SaslServerContext context, Symbol mechanism, ProtonBuffer initResponse) {
            }

            @Override
            public void handleSaslResponse(SaslServerContext context, ProtonBuffer response) {
            }
        });

        engine.pipeline().fireRead(new HeaderEnvelope(AMQPHeader.getSASLHeader()));

        assertThrows(IllegalStateException.class, () -> engine.saslDriver().client());

        assertTrue(saslHeaderRead.get(), "Did not receive a SASL Header");

        List<PerformativeEnvelope<?>> frames = testHandler.getFramesWritten();

        // We should get a SASL header indicating that the server accepted SASL
        assertEquals(1, frames.size(), "Sasl Anonymous exchange output not as expected");

        for (int i = 0; i < frames.size(); ++i) {
            PerformativeEnvelope<?> frame = frames.get(i);
            switch (i) {
                case 0:
                    assertTrue(frame.getFrameType() == HeaderEnvelope.HEADER_FRAME_TYPE);
                    HeaderEnvelope header = (HeaderEnvelope) frame;
                    assertTrue(header.getBody().isSaslHeader());
                    break;
                case 1:
                    assertTrue(frame.getFrameType() == SASLEnvelope.SASL_FRAME_TYPE);
                    break;
                default:
                    fail("Invalid Frame read during exchange: " + frame);
            }
        }
    }

    @Test
    public void testSaslAnonymousExchange() {
        final AtomicBoolean saslHeaderRead = new AtomicBoolean();

        final AtomicReference<String> clientHostname = new AtomicReference<>();
        final AtomicReference<Symbol> clientMechanism = new AtomicReference<>();
        final AtomicBoolean emptyResponse = new AtomicBoolean();

        Engine engine = createSaslServerEngine();

        engine.saslDriver().server().setListener(new SaslServerListener() {

            @Override
            public void handleSaslHeader(SaslServerContext context, AMQPHeader header) {
                if (header.isSaslHeader()) {
                    saslHeaderRead.set(true);
                }

                context.sendMechanisms(new Symbol[] { Symbol.valueOf("ANONYMOUS") });
            }

            @Override
            public void handleSaslInit(SaslServerContext context, Symbol mechanism, ProtonBuffer initResponse) {
                clientHostname.set(context.getHostname());
                clientMechanism.set(mechanism);
                if (initResponse.getReadableBytes() == 0) {
                    emptyResponse.set(true);
                }

                context.sendOutcome(SaslOutcome.SASL_OK, null);
            }

            @Override
            public void handleSaslResponse(SaslServerContext context, ProtonBuffer response) {

            }
        });

        // Check for Header processing
        engine.start().getEngine().pipeline().fireRead(new HeaderEnvelope(AMQPHeader.getSASLHeader()));

        assertTrue(saslHeaderRead.get(), "Did not receive a SASL Header");

        SaslInit clientInit = new SaslInit();
        clientInit.setHostname("HOST-NAME");
        clientInit.setMechanism(Symbol.valueOf("ANONYMOUS"));
        clientInit.setInitialResponse(new Binary(new byte[0]));

        // Check for Initial Response processing
        engine.pipeline().fireRead(new SASLEnvelope(clientInit));

        assertEquals("HOST-NAME", clientHostname.get());
        assertEquals(Symbol.valueOf("ANONYMOUS"), clientMechanism.get());
        assertTrue(emptyResponse.get(), "Response should be an empty byte array");

        List<PerformativeEnvelope<?>> frames = testHandler.getFramesWritten();

        assertEquals(3, frames.size(), "SASL Anonymous exchange output not as expected");

        for (int i = 0; i < frames.size(); ++i) {
            PerformativeEnvelope<?> frame = frames.get(i);
            SASLEnvelope saslFrame = null;

            switch (i) {
                case 0:
                    assertTrue(frame.getFrameType() == HeaderEnvelope.HEADER_FRAME_TYPE);
                    HeaderEnvelope header = (HeaderEnvelope) frame;
                    assertTrue(header.getBody().isSaslHeader());
                    break;
                case 1:
                    assertTrue(frame.getFrameType() == SASLEnvelope.SASL_FRAME_TYPE);
                    saslFrame = (SASLEnvelope) frame;
                    assertEquals(SaslPerformative.SaslPerformativeType.MECHANISMS, saslFrame.getBody().getPerformativeType());
                    SaslMechanisms mechanisms = (SaslMechanisms) saslFrame.getBody();
                    assertEquals(1, mechanisms.getSaslServerMechanisms().length);
                    assertEquals(Symbol.valueOf("ANONYMOUS"), mechanisms.getSaslServerMechanisms()[0]);
                    break;
                case 2:
                    assertTrue(frame.getFrameType() == SASLEnvelope.SASL_FRAME_TYPE);
                    saslFrame = (SASLEnvelope) frame;
                    assertEquals(SaslPerformative.SaslPerformativeType.OUTCOME, saslFrame.getBody().getPerformativeType());
                    org.apache.qpid.protonj2.types.security.SaslOutcome outcome =
                        (org.apache.qpid.protonj2.types.security.SaslOutcome) saslFrame.getBody();
                    assertEquals(SaslCode.OK, outcome.getCode());
                    break;
                default:
                    fail("Invalid Frame read during exchange: " + frame);
            }
        }
    }

    @Test
    public void testEngineFailedIfMoreSaslFramesArriveAfterSaslDone() {
        final AtomicBoolean saslHeaderRead = new AtomicBoolean();

        final AtomicReference<String> clientHostname = new AtomicReference<>();
        final AtomicReference<Symbol> clientMechanism = new AtomicReference<>();
        final AtomicBoolean emptyResponse = new AtomicBoolean();

        Engine engine = createSaslServerEngine();

        engine.saslDriver().server().setListener(new SaslServerListener() {

            @Override
            public void handleSaslHeader(SaslServerContext context, AMQPHeader header) {
                if (header.isSaslHeader()) {
                    saslHeaderRead.set(true);
                }

                context.sendMechanisms(new Symbol[] { Symbol.valueOf("ANONYMOUS") });
            }

            @Override
            public void handleSaslInit(SaslServerContext context, Symbol mechanism, ProtonBuffer initResponse) {
                clientHostname.set(context.getHostname());
                clientMechanism.set(mechanism);
                if (initResponse.getReadableBytes() == 0) {
                    emptyResponse.set(true);
                }

                context.sendOutcome(SaslOutcome.SASL_OK, null);
            }

            @Override
            public void handleSaslResponse(SaslServerContext context, ProtonBuffer response) {

            }
        });

        // Check for Header processing
        engine.start().getEngine().pipeline().fireRead(new HeaderEnvelope(AMQPHeader.getSASLHeader()));

        assertTrue(saslHeaderRead.get(), "Did not receive a SASL Header");

        SaslInit clientInit = new SaslInit();
        clientInit.setHostname("HOST-NAME");
        clientInit.setMechanism(Symbol.valueOf("ANONYMOUS"));
        clientInit.setInitialResponse(new Binary(new byte[0]));

        // Check for Initial Response processing
        engine.pipeline().fireRead(new SASLEnvelope(clientInit));

        assertEquals("HOST-NAME", clientHostname.get());
        assertEquals(Symbol.valueOf("ANONYMOUS"), clientMechanism.get());
        assertTrue(emptyResponse.get(), "Response should be an empty byte array");

        List<PerformativeEnvelope<?>> frames = testHandler.getFramesWritten();
        assertEquals(3, frames.size(), "SASL Anonymous exchange output not as expected");

        assertEquals(engine.saslDriver().getSaslState(), SaslState.AUTHENTICATED);

        // Fire another SASL frame and the engine should fail
        try {
            engine.pipeline().fireRead(new SASLEnvelope(clientInit));
            fail("Server should fail on unexpected SASL frames");
        } catch (EngineFailedException efe) {
        }

        assertTrue(engine.isFailed());
    }

    @Test
    public void testSaslHandlerDefaultsIntoServerMode() {
        Engine engine = createSaslCapableEngine();

        // Swallow incoming so we can test that an AMQP Header arrives after SASL
        engine.pipeline().addFirst("read-sink", new FrameReadSinkTransportHandler());

        // Check for Header processing
        engine.start().getEngine().pipeline().fireRead(new HeaderEnvelope(AMQPHeader.getSASLHeader()));

        SaslInit clientInit = new SaslInit();
        clientInit.setHostname("HOST-NAME");
        clientInit.setMechanism(Symbol.valueOf("ANONYMOUS"));
        clientInit.setInitialResponse(new Binary(new byte[0]));

        // Check for Initial Response processing
        engine.pipeline().fireRead(new SASLEnvelope(clientInit));

        List<PerformativeEnvelope<?>> frames = testHandler.getFramesWritten();

        assertEquals(3, frames.size(), "SASL Anonymous exchange output not as expected");

        engine.start().getEngine().pipeline().fireRead(new HeaderEnvelope(AMQPHeader.getAMQPHeader()));

        for (int i = 0; i < frames.size(); ++i) {
            PerformativeEnvelope<?> frame = frames.get(i);
            SASLEnvelope saslFrame = null;

            switch (i) {
                case 0:
                    assertTrue(frame.getFrameType() == HeaderEnvelope.HEADER_FRAME_TYPE);
                    HeaderEnvelope header1 = (HeaderEnvelope) frame;
                    assertTrue(header1.getBody().isSaslHeader());
                    break;
                case 1:
                    assertTrue(frame.getFrameType() == SASLEnvelope.SASL_FRAME_TYPE);
                    saslFrame = (SASLEnvelope) frame;
                    assertEquals(SaslPerformative.SaslPerformativeType.MECHANISMS, saslFrame.getBody().getPerformativeType());
                    SaslMechanisms mechanisms = (SaslMechanisms) saslFrame.getBody();
                    assertEquals(1, mechanisms.getSaslServerMechanisms().length);
                    assertEquals(Symbol.valueOf("PLAIN"), mechanisms.getSaslServerMechanisms()[0]);
                    break;
                case 2:
                    assertTrue(frame.getFrameType() == SASLEnvelope.SASL_FRAME_TYPE);
                    saslFrame = (SASLEnvelope) frame;
                    assertEquals(SaslPerformative.SaslPerformativeType.OUTCOME, saslFrame.getBody().getPerformativeType());
                    org.apache.qpid.protonj2.types.security.SaslOutcome outcome =
                        (org.apache.qpid.protonj2.types.security.SaslOutcome) saslFrame.getBody();
                    assertEquals(SaslCode.AUTH, outcome.getCode());
                    break;
                case 3:
                    assertTrue(frame.getFrameType() == HeaderEnvelope.HEADER_FRAME_TYPE);
                    HeaderEnvelope header2 = (HeaderEnvelope) frame;
                    assertFalse(header2.getBody().isSaslHeader());
                    break;
                default:
                    fail("Invalid Frame read during exchange: " + frame);
            }
        }
    }

    @Test
    public void testEngineFailedWhenNonSaslFrameWrittenDuringSaslExchange() {
        final AtomicBoolean saslHeaderRead = new AtomicBoolean();

        final AtomicReference<String> clientHostname = new AtomicReference<>();
        final AtomicReference<Symbol> clientMechanism = new AtomicReference<>();

        Engine engine = createSaslServerEngine();

        engine.saslDriver().server().setListener(new SaslServerListener() {

            @Override
            public void handleSaslHeader(SaslServerContext context, AMQPHeader header) {
                if (header.isSaslHeader()) {
                    saslHeaderRead.set(true);
                }

                context.sendMechanisms(new Symbol[] { Symbol.valueOf("ANONYMOUS") });
            }

            @Override
            public void handleSaslInit(SaslServerContext context, Symbol mechanism, ProtonBuffer initResponse) {
                clientHostname.set(context.getHostname());
                clientMechanism.set(mechanism);
            }

            @Override
            public void handleSaslResponse(SaslServerContext context, ProtonBuffer response) {

            }
        });

        // Check for Header processing
        engine.start().getEngine().pipeline().fireRead(new HeaderEnvelope(AMQPHeader.getSASLHeader()));

        assertTrue(saslHeaderRead.get(), "Did not receive a SASL Header");

        SaslInit clientInit = new SaslInit();
        clientInit.setHostname("HOST-NAME");
        clientInit.setMechanism(Symbol.valueOf("ANONYMOUS"));
        clientInit.setInitialResponse(new Binary(new byte[0]));

        // Check for Initial Response processing
        engine.pipeline().fireRead(new SASLEnvelope(clientInit));

        assertEquals("HOST-NAME", clientHostname.get());
        assertEquals(Symbol.valueOf("ANONYMOUS"), clientMechanism.get());

        List<PerformativeEnvelope<?>> frames = testHandler.getFramesWritten();

        assertEquals(2, frames.size(), "SASL Anonymous exchange output not as expected");

        try {
            engine.pipeline().fireWrite(AMQPPerformativeEnvelopePool.outgoingEnvelopePool().take(new Open(), 0, null));
        } catch (ProtocolViolationException pvex) {}

        assertTrue(engine.isFailed());

        for (int i = 0; i < frames.size(); ++i) {
            PerformativeEnvelope<?> frame = frames.get(i);
            SASLEnvelope saslFrame = null;

            switch (i) {
                case 0:
                    assertTrue(frame.getFrameType() == HeaderEnvelope.HEADER_FRAME_TYPE);
                    HeaderEnvelope header = (HeaderEnvelope) frame;
                    assertTrue(header.getBody().isSaslHeader());
                    break;
                case 1:
                    assertTrue(frame.getFrameType() == SASLEnvelope.SASL_FRAME_TYPE);
                    saslFrame = (SASLEnvelope) frame;
                    assertEquals(SaslPerformative.SaslPerformativeType.MECHANISMS, saslFrame.getBody().getPerformativeType());
                    SaslMechanisms mechanisms = (SaslMechanisms) saslFrame.getBody();
                    assertEquals(1, mechanisms.getSaslServerMechanisms().length);
                    assertEquals(Symbol.valueOf("ANONYMOUS"), mechanisms.getSaslServerMechanisms()[0]);
                    break;
                default:
                    fail("Invalid Frame read during exchange: " + frame);
            }
        }
    }

    private Engine createSaslServerEngine() {
        ProtonEngine engine = new ProtonEngine();

        engine.pipeline().addLast("sasl", new ProtonSaslHandler());
        engine.pipeline().addLast("test", testHandler);
        engine.pipeline().addLast("write-sink", new FrameWriteSinkTransportHandler());

        // Ensure engine SASL driver is configured for server mode.
        engine.saslDriver().server();

        return engine;
    }

    private Engine createSaslClientEngine() {
        ProtonEngine engine = new ProtonEngine();

        engine.pipeline().addLast("sasl", new ProtonSaslHandler());
        engine.pipeline().addLast("test", testHandler);
        engine.pipeline().addLast("write-sink", new FrameWriteSinkTransportHandler());

        // Ensure engine SASL driver is configured for client mode.
        engine.saslDriver().client();

        return engine;
    }

    private Engine createSaslCapableEngine() {
        ProtonEngine engine = new ProtonEngine();

        engine.pipeline().addLast("sasl", new ProtonSaslHandler());
        engine.pipeline().addLast("test", testHandler);
        engine.pipeline().addLast("write-sink", new FrameWriteSinkTransportHandler());

        return engine;
    }
}
