package org.messaginghub.amqperative;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton4j.amqp.driver.netty.NettyTestPeer;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReceiverTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReceiverTest.class);

    @Test(timeout = 60000)
    public void testCreateReceiver() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectAttach().withRole(Role.RECEIVER).respond();  // TODO match other options
            peer.expectDetach().withClosed(true).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());

            connection.openFuture().get(10, TimeUnit.SECONDS);

            Session session = connection.openSession();
            session.openFuture().get(10, TimeUnit.SECONDS);

            Receiver receiver = session.createReceiver("test-queue");
            receiver.openFuture().get(10, TimeUnit.SECONDS);
            receiver.close().get(10, TimeUnit.SECONDS);

            connection.close().get(10, TimeUnit.SECONDS);

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            LOG.info("Receiver test completed normally");
        }
    }
}
