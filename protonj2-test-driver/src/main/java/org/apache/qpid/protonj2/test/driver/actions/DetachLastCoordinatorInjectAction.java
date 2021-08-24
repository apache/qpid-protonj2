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
import org.apache.qpid.protonj2.test.driver.LinkTracker;
import org.apache.qpid.protonj2.test.driver.codec.transport.Role;

/**
 * Detach actions that ignores any other handle or channel configuration
 * and specifically targets the most recently created Coordinator Link.
 */
public class DetachLastCoordinatorInjectAction extends DetachInjectAction {

    /**
     * @param driver
     * 		The test driver that is linked to this inject action.
     */
    public DetachLastCoordinatorInjectAction(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    protected void beforeActionPerformed(AMQPTestDriver driver) {
        LinkTracker tracker = driver.sessions().getLastOpenedCoordinator();

        if (tracker == null) {
            throw new AssertionError("Cannot send coordinator detach as scripted, no active coordinator found.");
        }

        onChannel(tracker.getSession().getLocalChannel().intValue());

        if (!tracker.isLocallyAttached()) {
            AttachInjectAction attach = new AttachInjectAction(driver);

            attach.onChannel(onChannel());
            attach.withName(tracker.getName());
            attach.withSource(tracker.getRemoteSource());
            if (tracker.getRemoteTarget() != null) {
                attach.withTarget(tracker.getRemoteTarget());
            } else {
                attach.withTarget(tracker.getRemoteCoordinator());
            }

            if (tracker.isSender()) {
                attach.withRole(Role.SENDER);
                // Signal that a detach is incoming since an error was set
                // the action will not override an explicitly null source.
                if (getPerformative().getError() != null) {
                    attach.withNullSource();
                }
            } else {
                attach.withRole(Role.RECEIVER);
                // Signal that a detach is incoming since an error was set
                // the action will not override an explicitly null target.
                if (getPerformative().getError() != null) {
                    if (getPerformative().getError() != null) {
                        attach.withNullTarget();
                    }
                }
            }

            attach.perform(driver);
        }

        getPerformative().setHandle(tracker.getHandle());
    }
}
