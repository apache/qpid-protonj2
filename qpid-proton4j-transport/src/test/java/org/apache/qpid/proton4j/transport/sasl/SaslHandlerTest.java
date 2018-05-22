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
package org.apache.qpid.proton4j.transport.sasl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.security.SaslCode;
import org.apache.qpid.proton4j.amqp.security.SaslInit;
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.transport.Frame;
import org.apache.qpid.proton4j.transport.HeaderFrame;
import org.apache.qpid.proton4j.transport.SaslFrame;
import org.apache.qpid.proton4j.transport.Transport;
import org.apache.qpid.proton4j.transport.impl.ProtonTransport;
import org.apache.qpid.proton4j.transport.sasl.SaslConstants.SaslOutcomes;
import org.apache.qpid.proton4j.transport.util.TestSupportTransportHandler;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests SASL Handling by the SaslHandler TransportHandler class.
 */
public class SaslHandlerTest {

    private TestSupportTransportHandler testHandler;

    @Before
    public void setUp() {
        testHandler = new TestSupportTransportHandler();
    }

    @Test
    public void testSaslRejectsAMQPHeader() {
        final AtomicBoolean headerRead = new AtomicBoolean();

        Transport transport = createSaslServerTransport(new SaslServerListener() {

            @Override
            public void onSaslResponse(SaslServerContext context, Binary response) {
            }

            @Override
            public void onSaslInit(SaslServerContext context, Binary initResponse) {
            }

            @Override
            public void onSaslHeader(SaslServerContext context, AMQPHeader header) {
                headerRead.set(true);
            }

            @Override
            public void initialize(SaslServerContext context) {
            }
        });

        transport.getPipeline().fireHeaderFrame(new HeaderFrame(AMQPHeader.getRawAMQPHeader()));

        assertFalse("Should not receive a Header", headerRead.get());

        List<Frame<?>> frames = testHandler.getFramesWritten();

        assertEquals("Sasl Anonymous exchange output not as expected", 1, frames.size());

        for (int i = 0; i < frames.size(); ++i) {
            Frame<?> frame = frames.get(i);
            switch (i) {
                case 0:
                    assertTrue(frame.getType() == HeaderFrame.HEADER_FRAME_TYPE);
                    HeaderFrame header = (HeaderFrame) frame;
                    assertTrue(header.getBody().isSaslHeader());
                    break;
                default:
                    fail("Invalid Frame read during exchange: " + frame);
            }
        }
    }

    @Test
    public void testExchangeSaslHeader() {
        final AtomicBoolean saslHeaderRead = new AtomicBoolean();

        Transport transport = createSaslServerTransport(new SaslServerListener() {

            @Override
            public void onSaslResponse(SaslServerContext context, Binary response) {
            }

            @Override
            public void onSaslInit(SaslServerContext context, Binary initResponse) {
            }

            @Override
            public void onSaslHeader(SaslServerContext context, AMQPHeader header) {
                if (header.isSaslHeader()) {
                    saslHeaderRead.set(true);
                }
            }

            @Override
            public void initialize(SaslServerContext context) {
                // Server must advertise currently configured mechanisms
                context.setMechanisms("ANONYMOUS");
            }
        });

        transport.getPipeline().fireHeaderFrame(new HeaderFrame(AMQPHeader.getSASLHeader()));

        assertTrue("Did not receive a SASL Header", saslHeaderRead.get());

        List<Frame<?>> frames = testHandler.getFramesWritten();

        // We should get a header and the server should send its mechanisms
        assertEquals("Sasl Anonymous exchange output not as expected", 2, frames.size());

        for (int i = 0; i < frames.size(); ++i) {
            Frame<?> frame = frames.get(i);
            switch (i) {
                case 0:
                    assertTrue(frame.getType() == HeaderFrame.HEADER_FRAME_TYPE);
                    HeaderFrame header = (HeaderFrame) frame;
                    assertTrue(header.getBody().isSaslHeader());
                    break;
                case 1:
                    assertTrue(frame.getType() == SaslFrame.SASL_FRAME_TYPE);
                    break;
                default:
                    fail("Invalid Frame read during exchange: " + frame);
            }
        }
    }

    @Test
    public void testSaslAnonymousExchange() {
        final AtomicBoolean saslHeaderRead = new AtomicBoolean();

        final AtomicReference<String> clientHostname = new AtomicReference<String>();
        final AtomicReference<String> clientMechanism = new AtomicReference<String>();
        final AtomicBoolean emptyResponse = new AtomicBoolean();

        Transport transport = createSaslServerTransport(new SaslServerListener() {

            @Override
            public void onSaslResponse(SaslServerContext context, Binary response) {
            }

            @Override
            public void onSaslInit(SaslServerContext context, Binary initResponse) {
                clientHostname.set(context.getClientHostname());
                clientMechanism.set(context.getClientMechanism());
                if (initResponse.getLength() == 0) {
                    emptyResponse.set(true);
                }

                context.setOutcome(SaslOutcomes.PN_SASL_OK);
            }

            @Override
            public void onSaslHeader(SaslServerContext context, AMQPHeader header) {
                if (header.isSaslHeader()) {
                    saslHeaderRead.set(true);
                }
            }

            @Override
            public void initialize(SaslServerContext context) {
                // Server must advertise currently configured mechanisms
                context.setMechanisms("ANONYMOUS");
            }
        });

        // Check for Header processing
        transport.getPipeline().fireHeaderFrame(new HeaderFrame(AMQPHeader.getSASLHeader()));

        assertTrue("Did not receive a SASL Header", saslHeaderRead.get());

        SaslInit clientInit = new SaslInit();
        clientInit.setHostname("HOST-NAME");
        clientInit.setMechanism(Symbol.valueOf("ANONYMOUS"));
        clientInit.setInitialResponse(new Binary(new byte[0]));

        // Check for Initial Response processing
        transport.getPipeline().fireSaslFrame(new SaslFrame(clientInit, null));

        assertEquals("HOST-NAME", clientHostname.get());
        assertEquals("ANONYMOUS", clientMechanism.get());
        assertTrue("Response should be an empty byte array", emptyResponse.get());

        List<Frame<?>> frames = testHandler.getFramesWritten();

        assertEquals("Sasl Anonymous exchange output not as expected", 3, frames.size());

        for (int i = 0; i < frames.size(); ++i) {
            Frame<?> frame = frames.get(i);
            SaslFrame saslFrame = null;

            switch (i) {
                case 0:
                    assertTrue(frame.getType() == HeaderFrame.HEADER_FRAME_TYPE);
                    HeaderFrame header = (HeaderFrame) frame;
                    assertTrue(header.getBody().isSaslHeader());
                    break;
                case 1:
                    assertTrue(frame.getType() == SaslFrame.SASL_FRAME_TYPE);
                    saslFrame = (SaslFrame) frame;
                    assertEquals(SaslPerformative.SaslPerformativeType.MECHANISMS, saslFrame.getBody().getPerformativeType());
                    SaslMechanisms mechanisms = (SaslMechanisms) saslFrame.getBody();
                    assertEquals(1, mechanisms.getSaslServerMechanisms().length);
                    assertEquals(Symbol.valueOf("ANONYMOUS"), mechanisms.getSaslServerMechanisms()[0]);
                    break;
                case 2:
                    assertTrue(frame.getType() == SaslFrame.SASL_FRAME_TYPE);
                    saslFrame = (SaslFrame) frame;
                    assertEquals(SaslPerformative.SaslPerformativeType.OUTCOME, saslFrame.getBody().getPerformativeType());
                    SaslOutcome outcome = (SaslOutcome) saslFrame.getBody();
                    assertEquals(SaslCode.OK, outcome.getCode());
                    break;
                default:
                    fail("Invalid Frame read during exchange: " + frame);
            }
        }
    }

    private Transport createSaslServerTransport(SaslServerListener listener) {
        ProtonTransport transport = new ProtonTransport();

        // TODO - Add AMQP handling layer
        transport.getPipeline().addLast("sasl", SaslHandler.server(listener));
        transport.getPipeline().addLast("test", testHandler);

        return transport;
    }
}
