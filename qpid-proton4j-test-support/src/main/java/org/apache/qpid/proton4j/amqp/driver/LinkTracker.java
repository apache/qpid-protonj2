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

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Source;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Target;
import org.apache.qpid.proton4j.amqp.driver.codec.transactions.Coordinator;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;

/**
 * Tracks information about links that are opened be the client under test.
 */
public class LinkTracker {

    private final SessionTracker session;
    private final Attach attach;

    public LinkTracker(SessionTracker session, Attach attach) {
        this.session = session;
        this.attach = attach;
    }

    public SessionTracker getSession() {
        return session;
    }

    public String getName() {
        return attach.getName();
    }

    public Role getRole() {
        return isSender() ? Role.SENDER : Role.RECEIVER;
    }

    public SenderSettleMode getSndSettleMode() {
        return attach.getSndSettleMode() != null ? SenderSettleMode.valueOf(attach.getSndSettleMode()) : SenderSettleMode.MIXED;
    }

    public ReceiverSettleMode getRcvSettleMode() {
        return attach.getRcvSettleMode() != null ? ReceiverSettleMode.valueOf(attach.getRcvSettleMode()) : ReceiverSettleMode.FIRST;
    }

    public UnsignedInteger getHandle() {
        return attach.getHandle();
    }

    public Source getSource() {
        return attach.getSource();
    }

    public Target getTarget() {
        return attach.getTarget() instanceof Target ? (Target) attach.getTarget() : null;
    }

    public Coordinator getCoordinator() {
        return attach.getTarget() instanceof Coordinator ? (Coordinator) attach.getTarget() : null;
    }

    public boolean isSender() {
        return Role.RECEIVER.getValue() == attach.getRole();
    }

    public boolean isReceiver() {
        return Role.SENDER.getValue() == attach.getRole();
    }
}
