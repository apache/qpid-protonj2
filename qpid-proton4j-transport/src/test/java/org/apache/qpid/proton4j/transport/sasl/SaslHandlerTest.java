package org.apache.qpid.proton4j.transport.sasl;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.transport.HeaderFrame;
import org.apache.qpid.proton4j.transport.Transport;
import org.apache.qpid.proton4j.transport.impl.ProtonTransport;
import org.apache.qpid.proton4j.transport.util.TestSupportTransportHandler;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests SASL Handling by the SaslHandler TransportHandler class.
 */
public class SaslHandlerTest {

    @Ignore
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
                saslHeaderRead.set(true);
            }

            @Override
            public void initialize(SaslServerContext context) {
            }
        });

        transport.getPipeline().fireHeaderFrame(new HeaderFrame(AMQPHeader.getSASLHeader()));

        assertTrue("Did not receive a SASL Header", saslHeaderRead.get());
    }

    private Transport createSaslServerTransport(SaslServerListener listener) {
        ProtonTransport transport = new ProtonTransport();

        // TODO - Add AMQP handling layer
        transport.getPipeline().addLast("sasl", SaslHandler.server(listener));
        transport.getPipeline().addLast("test", new TestSupportTransportHandler());

        return transport;
    }
}
