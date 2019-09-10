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
package org.apache.qpid.proton4j.amqp.driver.actions;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.ScriptedAction;

/**
 * Action that will signal when the script has completed all expectations and
 * scripted actions or when a configured timeout occurs.
 */
public class ScriptCompleteAction implements ScriptedAction {

    protected final AMQPTestDriver driver;
    protected final CountDownLatch complete = new CountDownLatch(1);

    public ScriptCompleteAction(AMQPTestDriver driver) {
        this.driver = driver;
    }

    @Override
    public ScriptCompleteAction now() {
        complete.countDown();
        return this;
    }

    @Override
    public ScriptCompleteAction queue() {
        driver.addScriptedElement(this);
        return this;
    }

    @Override
    public ScriptCompleteAction later(int delay) {
        driver.afterDelay(delay, this);
        return this;
    }

    @Override
    public ScriptCompleteAction perform(AMQPTestDriver driver) {
        complete.countDown();
        return this;
    }

    public void await() throws InterruptedException {
        complete.await();
    }

    public void await(long timeout, TimeUnit units ) throws InterruptedException {
        if (!complete.await(timeout, units)) {
            throw new AssertionError("Timed out waiting for scripted expectations to be met", new TimeoutException());
        }
    }
}
