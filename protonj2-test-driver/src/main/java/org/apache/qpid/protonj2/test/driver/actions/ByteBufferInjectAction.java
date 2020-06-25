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
package org.apache.qpid.protonj2.test.driver.actions;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.ScriptedAction;

import io.netty.buffer.ByteBuf;

/**
 * Scripted action that will write the contents of a given buffer out through the driver.
 */
public class ByteBufferInjectAction implements ScriptedAction {

    private final ByteBuf buffer;
    private final AMQPTestDriver driver;

    public ByteBufferInjectAction(AMQPTestDriver driver, ByteBuf buffer) {
        this.buffer = buffer;
        this.driver = driver;
    }

    @Override
    public ByteBufferInjectAction perform(AMQPTestDriver driver) {
        driver.sendBytes(buffer);
        return this;
    }

    @Override
    public ByteBufferInjectAction now() {
        perform(driver);
        return this;
    }

    @Override
    public ByteBufferInjectAction later(int delay) {
        driver.afterDelay(delay, this);
        return this;
    }

    @Override
    public ByteBufferInjectAction queue() {
        driver.addScriptedElement(this);
        return this;
    }
}
