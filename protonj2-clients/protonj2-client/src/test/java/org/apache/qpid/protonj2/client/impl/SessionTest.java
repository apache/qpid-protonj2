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
package org.apache.qpid.protonj2.client.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.Session;
import org.apache.qpid.protonj2.client.SessionOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIOException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.apache.qpid.protonj2.test.driver.netty.NettyTestPeer;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test behaviors of the Session API
 */
@Timeout(20)
public class SessionTest extends ImperativeClientTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(SessionTest.class);

    @Test
    public void testSessionOpenTimeoutWhenNoRemoteBeginArrivesTimeout() throws Exception {
        doTestSessionOpenTimeoutWhenNoRemoteBeginArrives(true);
    }

    @Test
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

    @Test
    public void testSessionOpenWaitWithTimeoutCanceledWhenConnectionDrops() throws Exception {
        doTestSessionOpenWaitCanceledWhenConnectionDrops(true);
    }

    @Test
    public void testSessionOpenWaitWithNoTimeoutCanceledWhenConnectionDrops() throws Exception {
        doTestSessionOpenWaitCanceledWhenConnectionDrops(false);
    }

    private void doTestSessionOpenWaitCanceledWhenConnectionDrops(boolean timeout) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin();
            peer.dropAfterLastHandler(10);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();

            try {
                if (timeout) {
                    session.openFuture().get(10, TimeUnit.SECONDS);
                } else {
                    session.openFuture().get();
                }

                fail("Session Open should wait should abort when connection drops.");
            } catch (ExecutionException error) {
                LOG.info("Session open failed with error: ", error);
                assertTrue(error.getCause() instanceof ClientIOException);
            }

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSessionCloseTimeoutWhenNoRemoteEndArrivesTimeout() throws Exception {
        doTestSessionCloseTimeoutWhenNoRemoteEndArrives(true);
    }

    @Test
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

    @Test
    public void testSessionCloseWaitWithTimeoutCanceledWhenConnectionDrops() throws Exception {
        doTestSessionCloseWaitCanceledWhenConnectionDrops(true);
    }

    @Test
    public void testSessionCloseWaitWithNoTimeoutCanceledWhenConnectionDrops() throws Exception {
        doTestSessionCloseWaitCanceledWhenConnectionDrops(false);
    }

    private void doTestSessionCloseWaitCanceledWhenConnectionDrops(boolean timeout) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectEnd();
            peer.dropAfterLastHandler(10);
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            SessionOptions options = new SessionOptions();
            options.closeTimeout(75);
            Session session = connection.openSession(options).openFuture().get();

            try {
                if (timeout) {
                    session.close().get(10, TimeUnit.SECONDS);
                } else {
                    session.close().get();
                }
            } catch (ExecutionException error) {
                fail("Session Close should complete when parent connection drops.");
            }

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSessionCloseGetsResponseWithErrorDoesNotThrowTimedGet() throws Exception {
        doTestSessionCloseGetsResponseWithErrorThrows(true);
    }

    @Test
    public void testConnectionCloseGetsResponseWithErrorDoesNotThrowUntimedGet() throws Exception {
        doTestSessionCloseGetsResponseWithErrorThrows(false);
    }

    protected void doTestSessionCloseGetsResponseWithErrorThrows(boolean tiemout) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectEnd().respond().withErrorCondition(AmqpError.INTERNAL_ERROR.toString(), "Something odd happened.");
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
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

    @Test
    public void testSessionGetRemotePropertiesWaitsForRemoteBegin() throws Exception {
        tryReadSessionRemoteProperties(true);
    }

    @Test
    public void testSessionGetRemotePropertiesFailsAfterOpenTimeout() throws Exception {
        tryReadSessionRemoteProperties(false);
    }

    private void tryReadSessionRemoteProperties(boolean beginResponse) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().openTimeout(100);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            Session session = connection.openSession();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            Map<String, Object> expectedProperties = new HashMap<>();
            expectedProperties.put("TEST", "test-property");

            if (beginResponse) {
                peer.expectEnd().respond();
                peer.respondToLastBegin().withProperties(expectedProperties).later(10);
            } else {
                peer.expectEnd();
            }

            if (beginResponse) {
                assertNotNull(session.properties(), "Remote should have responded with a remote properties value");
                assertEquals(expectedProperties, session.properties());
            } else {
                try {
                    session.properties();
                    fail("Should failed to get remote state due to no begin response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            try {
                session.close().get();
            } catch (ExecutionException ex) {
                LOG.debug("Caught unexpected exception from close call", ex);
                fail("Should not fail close when connection not closed and end was sent");
            }

            peer.expectClose().respond();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSessionGetRemoteOfferedCapabilitiesWaitsForRemoteBegin() throws Exception {
        tryReadSessionRemoteOfferedCapabilities(true);
    }

    @Test
    public void testSessionGetRemoteOfferedCapabilitiesFailsAfterOpenTimeout() throws Exception {
        tryReadSessionRemoteOfferedCapabilities(false);
    }

    private void tryReadSessionRemoteOfferedCapabilities(boolean beginResponse) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().openTimeout(100);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            connection.openFuture().get();

            Session session = connection.openSession();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (beginResponse) {
                peer.expectEnd().respond();
                peer.respondToLastBegin().withOfferedCapabilities("transactions").later(10);
            } else {
                peer.expectEnd();
            }

            if (beginResponse) {
                assertNotNull(session.offeredCapabilities(), "Remote should have responded with a remote offered Capabilities value");
                assertEquals(1, session.offeredCapabilities().length);
                assertEquals("transactions", session.offeredCapabilities()[0]);
            } else {
                try {
                    session.offeredCapabilities();
                    fail("Should failed to get remote state due to no begin response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            try {
                session.close().get();
            } catch (ExecutionException ex) {
                LOG.debug("Caught unexpected exception from close call", ex);
                fail("Should not fail close when connection not closed and end was sent");
            }

            peer.expectClose().respond();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSessionGetRemoteDesiredCapabilitiesWaitsForRemoteBegin() throws Exception {
        tryReadSessionRemoteDesiredCapabilities(true);
    }

    @Test
    public void testSessionGetRemoteDesiredCapabilitiesFailsAfterOpenTimeout() throws Exception {
        tryReadSessionRemoteDesiredCapabilities(false);
    }

    private void tryReadSessionRemoteDesiredCapabilities(boolean beginResponse) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Connect test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            ConnectionOptions options = new ConnectionOptions().openTimeout(100);
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            Session session = connection.openSession();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (beginResponse) {
                peer.expectEnd().respond();
                peer.respondToLastBegin().withDesiredCapabilities("Error-Free").later(10);
            } else {
                peer.expectEnd();
            }

            if (beginResponse) {
                assertNotNull(session.desiredCapabilities(), "Remote should have responded with a remote desired Capabilities value");
                assertEquals(1, session.desiredCapabilities().length);
                assertEquals("Error-Free", session.desiredCapabilities()[0]);
            } else {
                try {
                    session.desiredCapabilities();
                    fail("Should failed to get remote state due to no begin response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            try {
                session.close().get();
            } catch (ExecutionException ex) {
                LOG.debug("Caught unexpected exception from close call", ex);
                fail("Should not fail close to when connection not closed and end sent");
            }

            peer.expectClose().respond();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testQuickOpenCloseWhenNoBeginResponseFailsFastOnOpenTimeout() throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin();
            peer.expectEnd();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            ConnectionOptions options = new ConnectionOptions();
            options.openTimeout(100);
            options.closeTimeout(TimeUnit.HOURS.toMillis(1));  // Test would timeout if waited on.

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort(), options);
            connection.openFuture().get();

            try {
                connection.openSession().close().get();
            } catch (ExecutionException error) {
                fail("Should not fail when waiting on close with quick open timeout");
            }

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCloseWithErrorCondition() throws Exception {
        final String condition = "amqp:precondition-failed";
        final String description = "something bad happened.";

        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectEnd().withError(condition, description).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();

            LOG.info("Test started, peer listening on: {}", remoteURI);

            Client container = Client.create();
            Connection connection = container.connect(remoteURI.getHost(), remoteURI.getPort());
            Session session = connection.openSession();

            session.openFuture().get();
            session.close(ErrorCondition.create(condition, description, null));

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
