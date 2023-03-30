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
import org.apache.qpid.protonj2.test.driver.ProtonTestPeer;
import org.apache.qpid.protonj2.test.driver.ScriptedAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Action that drops the netty connection to the remote once invoked.
 */
public class PeerShutdownAction implements ScriptedAction {

    private static final Logger LOG = LoggerFactory.getLogger(PeerShutdownAction.class);

    private final ProtonTestPeer peer;
    private int delay = -1;

    public PeerShutdownAction(ProtonTestPeer peer) {
        this.peer = peer;
    }

    @Override
    public ScriptedAction now() {
        LOG.info("Peer Shutdown Action closing test peer as scripted");
        peer.close();
        return this;
    }

    @Override
    public ScriptedAction later(int waitTime) {
        peer.getDriver().afterDelay(delay, this);
        return this;
    }

    @Override
    public ScriptedAction queue() {
        peer.getDriver().addScriptedElement(this);
        return this;
    }

    @Override
    public ScriptedAction perform(AMQPTestDriver driver) {
        if (afterDelay() > 0) {
            driver.afterDelay(afterDelay(), new ScriptedAction() {

                @Override
                public ScriptedAction queue() {
                    return this;
                }

                @Override
                public ScriptedAction perform(AMQPTestDriver driver) {
                    return PeerShutdownAction.this.now();
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

    public int afterDelay() {
        return delay;
    }

    public PeerShutdownAction afterDelay(int delay) {
        this.delay = delay;
        return this;
    }
}
