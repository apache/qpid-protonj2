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
package org.apache.qpid.protonj2.test.driver;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.protonj2.test.driver.actions.ConnectionDropAction;
import org.apache.qpid.protonj2.test.driver.actions.PeerShutdownAction;

/**
 * Abstract base class that is implemented by all the AMQP v1.0 test peer
 * implementations to provide a consistent interface for the test driver
 * classes to interact with.
 */
public abstract class ProtonTestPeer extends ScriptWriter implements AutoCloseable {

    protected final AtomicBoolean closed = new AtomicBoolean();

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            processPeerShutdownRequest();
        }
    }

    /**
     * Disconnect any current active connection to this peer, or from this peer if it
     * is a client type connection. Depending on the peer implementation this could be
     * a terminal action. A server implementation should allow for a new connection to
     * be made to it following dropping any existing client connection.
     */
    public void dropConnection() {
        if (closed.compareAndSet(false, true)) {
            processCloseConnectionRequest();
        }
    }

    /**
     * Waits for the test script to complete and ignores any errors seen. This method is
     * generally only useful for scripts that complete other specific waits and then want
     * to wait for a shutdown sequence without scripting an exact order of events.
     */
    public void waitForScriptToCompleteIgnoreErrors() {
        getDriver().waitForScriptToCompleteIgnoreErrors();
    }

    /**
     * Waits for the configured test script to complete and will throw any errors
     * from the expected script as {@link AssertionError} exceptions.
     */
    public void waitForScriptToComplete() {
        getDriver().waitForScriptToComplete();
    }

    /**
     * Waits for the configured test script to complete and will throw any errors
     * from the expected script as {@link AssertionError} exceptions.
     *
     * @param timeout
     * 		The time to wait in milliseconds before failing the wait.
     */
    public void waitForScriptToComplete(long timeout) {
        getDriver().waitForScriptToComplete(timeout);
    }

    /**
     * Waits for the configured test script to complete and will throw any errors
     * from the expected script as {@link AssertionError} exceptions.
     *
     * @param timeout
     * 		The time to wait before failing the wait.
     * @param units
     * 		The units to use for the given time interval.
     */
    public void waitForScriptToComplete(long timeout, TimeUnit units) {
        getDriver().waitForScriptToComplete(timeout, units);
    }

    /**
     * @return the total number of AMQP idle frames that were received.
     */
    public int getEmptyFrameCount() {
        return getDriver().getEmptyFrameCount();
    }

    /**
     * @return the total number of AMQP performatives that were read so far.
     */
    public int getPerformativeCount() {
        return getDriver().getPerformativeCount();
    }

    /**
     * @return the total number of SASL performatives that were read so far.
     */
    public int getSaslPerformativeCount() {
        return getDriver().getSaslPerformativeCount();
    }

    /**
     * Drops the connection to the connected client immediately after the last handler that was
     * registered before this scripted action is queued. Depending on the test peer this action
     * could be a terminal one meaning no other scripted elements could be added and doing so might
     * result in wait for completion calls to hang. In general any server type peer should allow
     * reconnects after dropping a client connection but a client peer would be expected not to
     * allow and action following a drop.
     *
     * @return this test peer instance.
     */
    public ProtonTestPeer dropAfterLastHandler() {
        getDriver().addScriptedElement(new ConnectionDropAction(this));
        return this;
    }

    /**
     * Drops the connection to the connected client immediately after the last handler that was
     * registered before this scripted action is queued. Depending on the test peer this action
     * could be a terminal one meaning no other scripted elements could be added and doing so might
     * result in wait for completion calls to hang. In general any server type peer should allow
     * reconnects after dropping a client connection but a client peer would be expected not to
     * allow and action following a drop.
     *
     * @param delay
     *      The time in milliseconds to wait before running the action after the last handler is run.
     *
     * @return this test peer instance.
     */
    public ProtonTestPeer dropAfterLastHandler(int delay) {
        getDriver().addScriptedElement(new ConnectionDropAction(this).afterDelay(delay));
        return this;
    }

    /**
     * Shuts down the peer dropping and connections and rendering the peer not usable for
     * any new actions to be added.
     *
     * @return this test peer instance.
     */
    public ProtonTestPeer shutdownAfterLastHandler() {
        getDriver().addScriptedElement(new PeerShutdownAction(this));
        return this;
    }

    /**
     * Shuts down the peer dropping and connections and rendering the peer not usable for
     * any new actions to be added.
     *
     * @param delay
     *      The time in milliseconds to wait before running the action after the last handler is run.
     *
     * @return this test peer instance.
     */
    public ProtonTestPeer shutdownAfterLastHandler(int delay) {
        getDriver().addScriptedElement(new PeerShutdownAction(this).afterDelay(delay));
        return this;
    }

    protected abstract String getPeerName();

    protected abstract void processCloseConnectionRequest();

    protected abstract void processPeerShutdownRequest();

    protected abstract void processDriverOutput(ByteBuffer frame);

    protected abstract void processConnectionEstablished();

    protected abstract void processConnectionDropped();

    protected void checkClosed() {
        if (closed.get()) {
            throw new IllegalStateException("The test peer is closed");
        }
    }
}
