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
package org.apache.qpid.proton4j.amqp.driver;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * An in VM single threaded test driver used for testing Engine implementations
 * where all test operations will take place in a single thread of control.
 *
 * This class in mainly intended for use in JUnit tests of an Engine implementation
 * and not for use by client implementations where a socket based test peer would be
 * a more appropriate choice.
 */
public class ProtonTestPeer extends ScriptWriter implements Consumer<ProtonBuffer>, AutoCloseable {

    private final AMQPTestDriver driver;
    private final Consumer<ProtonBuffer> inputConsumer;
    private final AtomicBoolean closed = new AtomicBoolean();

    public ProtonTestPeer(Consumer<ProtonBuffer> frameSink) {
        this.driver = new AMQPTestDriver((frame) -> {
            processIncomingData(frame);
        }, null);

        this.inputConsumer = frameSink;
    }

    public int getEmptyFrameCount() {
        return driver.getEmptyFrameCount();
    }

    public int getPerformativeCount() {
        return driver.getPerformativeCount();
    }

    public int getSaslPerformativeCount() {
        return driver.getSaslPerformativeCount();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            processCloseRequest();
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void accept(ProtonBuffer frame) {
        driver.accept(frame);
    }

    //----- Test Completion API

    public void waitForScriptToCompleteIgnoreErrors() {
        driver.waitForScriptToCompleteIgnoreErrors();
    }

    public void waitForScriptToComplete() {
        driver.waitForScriptToComplete();
    }

    public void waitForScriptToComplete(long timeout) {
        driver.waitForScriptToComplete(timeout);
    }

    public void waitForScriptToComplete(long timeout, TimeUnit units) {
        driver.waitForScriptToComplete(timeout, units);
    }

    //----- Internal implementation which can be overridden

    protected void processCloseRequest() {
        // nothing to do in this peer implementation.
    }

    protected void processIncomingData(ProtonBuffer frame) {
        inputConsumer.accept(frame);
    }

    @Override
    protected AMQPTestDriver getDriver() {
        return driver;
    }
}
