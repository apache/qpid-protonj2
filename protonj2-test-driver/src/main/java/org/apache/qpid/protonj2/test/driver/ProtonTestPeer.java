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
            processCloseRequest();
        }
    }

    public void waitForScriptToCompleteIgnoreErrors() {
        getDriver().waitForScriptToCompleteIgnoreErrors();
    }

    public void waitForScriptToComplete() {
        getDriver().waitForScriptToComplete();
    }

    public void waitForScriptToComplete(long timeout) {
        getDriver().waitForScriptToComplete(timeout);
    }

    public void waitForScriptToComplete(long timeout, TimeUnit units) {
        getDriver().waitForScriptToComplete(timeout, units);
    }

    public int getEmptyFrameCount() {
        return getDriver().getEmptyFrameCount();
    }

    public int getPerformativeCount() {
        return getDriver().getPerformativeCount();
    }

    public int getSaslPerformativeCount() {
        return getDriver().getSaslPerformativeCount();
    }

    /**
     * Drops the connection to the connected client immediately after the last handler that was
     * registered before this scripted action is queued.  Adding any additional test scripting to
     * the test driver will either not be acted on or could cause the wait methods to not return
     * as they will never be invoked.
     *
     * @return this test peer instance.
     */
    public ProtonTestPeer dropAfterLastHandler() {
        getDriver().addScriptedElement(new ConnectionDropAction(this));
        return this;
    }

    /**
     * Drops the connection to the connected client immediately after the last handler that was
     * registered before this scripted action is queued.  Adding any additional test scripting to
     * the test driver will either not be acted on or could cause the wait methods to not return
     * as they will never be invoked.
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

    protected abstract String getPeerName();

    protected abstract void processCloseRequest();

    protected abstract void processDriverOutput(ByteBuffer frame);

    protected abstract void processConnectionEstablished();

    protected void checkClosed() {
        if (closed.get()) {
            throw new IllegalStateException("The test peer is closed");
        }
    }
}
