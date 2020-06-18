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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton4j.test.driver.netty.NettyTestPeer;
import org.apache.qpid.proton4j.types.transport.AmqpError;
import org.junit.Test;
import org.messaginghub.amqperative.exceptions.ClientException;
import org.messaginghub.amqperative.exceptions.ClientOperationTimedOutException;
import org.messaginghub.amqperative.test.AMQPerativeTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test behaviors of the Session API
 */
public class SessionTest extends AMQPerativeTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(SessionTest.class);

    @Test(timeout = 30000)
    public void testSessionOpenTimeoutWhenNoRemoteBeginArrivesTimeout() throws Exception {
        doTestSessionOpenTimeoutWhenNoRemoteBeginArrives(true);
    }

    @Test(timeout = 30000)
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

    @Test(timeout = 30000)
    public void testSessionCloseTimeoutWhenNoRemoteEndArrivesTimeout() throws Exception {
        doTestSessionCloseTimeoutWhenNoRemoteEndArrives(true);
    }

    @Test(timeout = 30000)
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

    @Test(timeout = 30000)
    public void testSessionCloseGetsResponseWithErrorDoesNotThrowTimedGet() throws Exception {
        doTestSessionCloseGetsResponseWithErrorThrows(true);
    }

    @Test(timeout = 30000)
    public void testConnectionCloseGetsResponseWithErrorDoesNotThrowUntimedGet() throws Exception {
        doTestSessionCloseGetsResponseWithErrorThrows(false);
    }

    protected void doTestSessionCloseGetsResponseWithErrorThrows(boolean tiemout) throws Exception {
        try (NettyTestPeer peer = new NettyTestPeer()) {
            peer.expectSASLAnonymousConnect();
            peer.expectOpen().respond();
            peer.expectBegin().respond();
            peer.expectEnd().respond().withErrorCondition(AmqpError.INTERNAL_ERROR, "Something odd happened.");
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

    @Test(timeout = 30000)
    public void testSessionGetRemotePropertiesWaitsForRemoteBegin() throws Exception {
        tryReadSessionRemoteProperties(true);
    }

    @Test(timeout = 30000)
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
            connection.openFuture().get();

            Session session = connection.openSession();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            Map<String, Object> expectedProperties = new HashMap<>();
            expectedProperties.put("TEST", "test-property");

            if (beginResponse) {
                peer.expectEnd().respond();
                peer.respondToLastBegin().withPropertiesMap(expectedProperties).later(10);
            } else {
                peer.expectEnd();
            }

            if (beginResponse) {
                assertNotNull("Remote should have responded with a remote properties value", session.properties());
                assertEquals(expectedProperties, session.properties());
            } else {
                try {
                    session.properties();
                    fail("Should failed to get remote state due to no begin response");
                } catch (ClientException ex) {
                    LOG.debug("Caught expected exception from blocking call", ex);
                }
            }

            if (beginResponse) {
                session.close().get();
            } else {
                try {
                    session.close().get();
                    fail("Should fail close to indicate remote misbehaving when connection not closed");
                } catch (ExecutionException ex) {
                    LOG.debug("Caught expected exception from close call", ex);
                }
            }

            peer.expectClose().respond();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testSessionGetRemoteOfferedCapabilitiesWaitsForRemoteBegin() throws Exception {
        tryReadSessionRemoteOfferedCapabilities(true);
    }

    @Test(timeout = 30000)
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
                assertNotNull("Remote should have responded with a remote offered Capabilities value", session.offeredCapabilities());
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

            if (beginResponse) {
                session.close().get();
            } else {
                try {
                    session.close().get();
                    fail("Should fail close to indicate remote misbehaving when connection not closed");
                } catch (ExecutionException ex) {
                    LOG.debug("Caught expected exception from close call", ex);
                }
            }

            peer.expectClose().respond();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
    public void testSessionGetRemoteDesiredCapabilitiesWaitsForRemoteBegin() throws Exception {
        tryReadSessionRemoteDesiredCapabilities(true);
    }

    @Test(timeout = 30000)
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
            connection.openFuture().get();

            Session session = connection.openSession();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            if (beginResponse) {
                peer.expectEnd().respond();
                peer.respondToLastBegin().withDesiredCapabilities("Error-Free").later(10);
            } else {
                peer.expectEnd();
            }

            if (beginResponse) {
                assertNotNull("Remote should have responded with a remote desired Capabilities value", session.desiredCapabilities());
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

            if (beginResponse) {
                session.close().get();
            } else {
                try {
                    session.close().get();
                    fail("Should fail close to indicate remote misbehaving when connection not closed");
                } catch (ExecutionException ex) {
                    LOG.debug("Caught expected exception from close call", ex);
                }
            }

            peer.expectClose().respond();
            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
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
                fail("Should fail quickly when waiting on close with quick open timeout");
            } catch (ExecutionException error) {
                // Expected so ignore any timeout based failures
                assertTrue(error.getCause() instanceof ClientOperationTimedOutException);
            }

            connection.close().get();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }

    @Test(timeout = 30000)
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
