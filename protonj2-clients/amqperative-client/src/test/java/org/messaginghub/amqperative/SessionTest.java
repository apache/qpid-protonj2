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
package org.messaginghub.amqperative;

import static org.junit.Assert.fail;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton4j.amqp.driver.netty.NettyTestPeer;
import org.apache.qpid.proton4j.amqp.transport.AmqpError;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.junit.Test;
import org.messaginghub.amqperative.test.AMQPerativeTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test behaviors of the Session API
 */
public class SessionTest extends AMQPerativeTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(SessionTest.class);

    @Test(timeout = 60000)
    public void testSessionOpenTimeoutWhenNoRemoteBeginArrivesTimeout() throws Exception {
        doTestSessionOpenTimeoutWhenNoRemoteBeginArrives(true);
    }

    @Test(timeout = 60000)
    public void testSessionOpenTimeoutWhenNoRemoteBeginArrivesNoTimeout() throws Exception {
        doTestSessionOpenTimeoutWhenNoRemoteBeginArrives(false);
    }

    private void doTestSessionOpenTimeoutWhenNoRemoteBeginArrives(boolean timeout) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin();
            peer.expectEnd();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            connection.openFuture().get(10, TimeUnit.SECONDS);

            SessionOptions options = new SessionOptions();
            options.openTimeout(75);
            Session session = connection.openSession(options);

            try {
                if (timeout) {
                    session.openFuture().get(10, TimeUnit.SECONDS);
                } else {
                    session.openFuture().get();
                }

                fail("Session Open should timeout when no Begin response and complete future with error.");
            } catch (Throwable error) {
                LOG.info("Session open failed with error: ", error);
            }

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 60000)
    public void testSessionCloseTimeoutWhenNoRemoteEndArrivesTimeout() throws Exception {
        doTestSessionCloseTimeoutWhenNoRemoteEndArrives(true);
    }

    @Test(timeout = 60000)
    public void testSessionCloseTimeoutWhenNoRemoteEndArrivesNoTimeout() throws Exception {
        doTestSessionCloseTimeoutWhenNoRemoteEndArrives(false);
    }

    private void doTestSessionCloseTimeoutWhenNoRemoteEndArrives(boolean timeout) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectEnd();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            connection.openFuture().get(10, TimeUnit.SECONDS);

            SessionOptions options = new SessionOptions();
            options.closeTimeout(75);
            Session session = connection.openSession(options).openFuture().get();

            try {
                if (timeout) {
                    session.close().get(10, TimeUnit.SECONDS);
                } else {
                    session.close().get();
                }

                fail("Close should throw an error if the Session end doesn't arrive in time");
            } catch (Throwable error) {
                LOG.info("Session close failed with error: ", error);
            }

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 60000)
    public void testSessionCloseGetsResponseWithErrorDoesNotThrowTimedGet() throws Exception {
        doTestSessionCloseGetsResponseWithErrorThrows(true);
    }

    @Test(timeout = 60000)
    public void testConnectionCloseGetsResponseWithErrorDoesNotThrowUntimedGet() throws Exception {
        doTestSessionCloseGetsResponseWithErrorThrows(false);
    }

    protected void doTestSessionCloseGetsResponseWithErrorThrows(boolean tiemout) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectEnd().respond().withErrorCondition(
                new ErrorCondition(AmqpError.INTERNAL_ERROR, "Something odd happened."));
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            connection.openFuture().get(10, TimeUnit.SECONDS);
            Session session = connection.openSession().openFuture().get();

            if (tiemout) {
                // Should close normally and not throw error as we initiated the close.
                session.close().get(10, TimeUnit.SECONDS);
            } else {
                // Should close normally and not throw error as we initiated the close.
                session.close().get();
            }

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
