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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * An in VM single threaded test driver used for testing Engine implementations
 * where all test operations will take place in a single thread of control.
 *
 * This class in mainly intended for use in JUnit tests of an Engine implementation
 * and not for use by client implementations where a socket based test peer would be
 * a more appropriate choice.
 */
public class ProtonTestConnector extends ProtonTestPeer implements Consumer<ByteBuffer> {

    private final AMQPTestDriver driver;
    private final Consumer<ByteBuffer> inputConsumer;

    public ProtonTestConnector(Consumer<ByteBuffer> frameSink) {
        this.driver = new AMQPTestDriver(getPeerName(), (frame) -> {
            processDriverOutput(frame);
        }, null);

        this.inputConsumer = frameSink;
    }

    @Override
    public String getPeerName() {
        return "InVMConnector";
    }

    @Override
    public void accept(ByteBuffer frame) {
        if (closed.get()) {
            throw new UncheckedIOException("Closed driver is not accepting any new input", new IOException());
        } else {
            driver.accept(frame);
        }
    }

    @Override
    public AMQPTestDriver getDriver() {
        return driver;
    }

    //----- Internal implementation which can be overridden

    @Override
    protected void processCloseConnectionRequest() {
        // nothing to do in this peer implementation.
    }

    @Override
    protected void processPeerShutdownRequest() {
        // nothing to do in this peer implementation.
    }

    @Override
    protected void processDriverOutput(ByteBuffer frame) {
        inputConsumer.accept(frame);
    }

    @Override
    protected void processConnectionEstablished() {
        driver.handleConnectedEstablished();
    }

    @Override
    protected void processConnectionDropped() {
        driver.handleConnectedDropped();
    }
}
