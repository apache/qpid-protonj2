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
package org.apache.qpid.proton4j.test.driver.actions;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.test.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.test.driver.ScriptedAction;
import org.apache.qpid.proton4j.test.driver.codec.primitives.DescribedType;
import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedShort;

/**
 * Abstract base used by inject actions of AMQP Performatives
 *
 * @param <P> the AMQP performative being sent.
 */
public abstract class AbstractPerformativeInjectAction<P extends DescribedType> implements ScriptedAction {

    public static final int CHANNEL_UNSET = -1;

    private final AMQPTestDriver driver;

    private int channel = CHANNEL_UNSET;
    private int delay = -1;

    public AbstractPerformativeInjectAction(AMQPTestDriver driver) {
        this.driver = driver;
    }

    @Override
    public final AbstractPerformativeInjectAction<P> now() {
        perform(driver);
        return this;
    }

    @Override
    public final AbstractPerformativeInjectAction<P> later(int delay) {
        driver.afterDelay(delay, this);
        return this;
    }

    @Override
    public final AbstractPerformativeInjectAction<P> queue() {
        driver.addScriptedElement(this);
        return this;
    }

    @Override
    public final AbstractPerformativeInjectAction<P> perform(AMQPTestDriver driver) {
        // Give actors a chance to prepare.
        beforeActionPerformed(driver);

        if (afterDelay() > 0) {
            driver.afterDelay(afterDelay(), new ScriptedAction() {

                @Override
                public ScriptedAction queue() {
                    return this;
                }

                @Override
                public ScriptedAction perform(AMQPTestDriver driver) {
                    driver.sendAMQPFrame(onChannel(), getPerformative(), getPayload());
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
            driver.sendAMQPFrame(onChannel(), getPerformative(), getPayload());
        }

        return this;
    }

    public int onChannel() {
        return channel;
    }

    public int afterDelay() {
        return delay;
    }

    public AbstractPerformativeInjectAction<?> afterDelay(int delay) {
        this.delay = delay;
        return this;
    }

    public AbstractPerformativeInjectAction<?> onChannel(int channel) {
        this.channel = channel;
        return this;
    }

    public AbstractPerformativeInjectAction<?> onChannel(UnsignedShort channel) {
        this.channel = channel.intValue();
        return this;
    }

    /**
     * @return the AMQP Performative that is to be sent as a result of this action.
     */
    public abstract P getPerformative();

    /**
     * @return the buffer containing the payload that should be sent as part of this action.
     */
    public ProtonBuffer getPayload() {
        return null;
    }

    protected void beforeActionPerformed(AMQPTestDriver driver) {
        // Subclass can override to modify driver of update performative state.
    }
}
