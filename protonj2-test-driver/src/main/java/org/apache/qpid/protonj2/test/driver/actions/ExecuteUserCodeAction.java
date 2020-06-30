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

import java.util.Objects;
import java.util.concurrent.ForkJoinPool;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.ScriptedAction;

/**
 * Runs the given user code either now, later or after other test expectations have occurred.
 *
 * The given action will be executed on the common ForkJoinPool to prevent any blocking
 * operations from affecting the test driver event loop itself.
 */
public class ExecuteUserCodeAction implements ScriptedAction {

    private final AMQPTestDriver driver;
    private final Runnable action;

    private int delay = -1;

    public ExecuteUserCodeAction(AMQPTestDriver driver, Runnable action) {
        Objects.requireNonNull(driver, "Test Driver to use cannot be null");
        Objects.requireNonNull(action, "Action to run cannot be null");

        this.driver = driver;
        this.action = action;
    }

    public int afterDelay() {
        return delay;
    }

    public ExecuteUserCodeAction afterDelay(int delay) {
        this.delay = delay;
        return this;
    }

    @Override
    public ExecuteUserCodeAction now() {
        ForkJoinPool.commonPool().execute(action);
        return this;
    }

    @Override
    public ExecuteUserCodeAction later(int delay) {
        driver.afterDelay(delay, this);
        return this;
    }

    @Override
    public ExecuteUserCodeAction queue() {
        driver.addScriptedElement(this);
        return this;
    }

    @Override
    public ExecuteUserCodeAction perform(AMQPTestDriver driver) {
        if (afterDelay() > 0) {
            driver.afterDelay(afterDelay(), new ScriptedAction() {

                @Override
                public ScriptedAction queue() {
                    return this;
                }

                @Override
                public ScriptedAction perform(AMQPTestDriver driver) {
                    now();
                    return this;
                }

                @Override
                public ScriptedAction now() {
                    return this;
                }

                @Override
                public ScriptedAction later(int waitTime) {
                    return this;
                }
            });
        } else {
            now();
        }

        return this;
    }
}
