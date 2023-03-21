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

import org.apache.qpid.protonj2.test.driver.codec.messaging.Source;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Target;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.transactions.Coordinator;
import org.apache.qpid.protonj2.test.driver.codec.transport.Attach;
import org.apache.qpid.protonj2.test.driver.codec.transport.Detach;
import org.apache.qpid.protonj2.test.driver.codec.transport.Flow;
import org.apache.qpid.protonj2.test.driver.codec.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.test.driver.codec.transport.Role;
import org.apache.qpid.protonj2.test.driver.codec.transport.SenderSettleMode;
import org.apache.qpid.protonj2.test.driver.codec.transport.Transfer;

/**
 * Tracks information about links that are opened be the client under test.
 */
public abstract class LinkTracker {

    private final SessionTracker session;

    private Attach remoteAttach;
    private Detach remoteDetach;

    private Attach localAttach;
    private Detach localDetach;

    public LinkTracker(SessionTracker session) {
        this.session = session;
    }

    public SessionTracker getSession() {
        return session;
    }

    public String getName() {
        if (remoteAttach != null) {
            return remoteAttach.getName();
        } else {
            return localAttach.getName();
        }
    }

    public Role getRole() {
        return isSender() ? Role.SENDER : Role.RECEIVER;
    }

    public SenderSettleMode getSenderSettleMode() {
        return localAttach.getSenderSettleMode() != null ? SenderSettleMode.valueOf(localAttach.getSenderSettleMode()) : SenderSettleMode.MIXED;
    }

    public ReceiverSettleMode getReceiverSettleMode() {
        return localAttach.getReceiverSettleMode() != null ? ReceiverSettleMode.valueOf(localAttach.getReceiverSettleMode()) : ReceiverSettleMode.FIRST;
    }

    public SenderSettleMode getRemoteSenderSettleMode() {
        return remoteAttach.getSenderSettleMode() != null ? SenderSettleMode.valueOf(remoteAttach.getSenderSettleMode()) : SenderSettleMode.MIXED;
    }

    public ReceiverSettleMode getRemoteReceiverSettleMode() {
        return remoteAttach.getReceiverSettleMode() != null ? ReceiverSettleMode.valueOf(remoteAttach.getReceiverSettleMode()) : ReceiverSettleMode.FIRST;
    }

    public UnsignedInteger getHandle() {
        return localAttach.getHandle();
    }

    public UnsignedInteger getRemoteHandle() {
        return remoteAttach.getHandle();
    }

    public Source getSource() {
        return localAttach.getSource();
    }

    public Target getTarget() {
        return localAttach.getTarget() instanceof Target ? (Target) localAttach.getTarget() : null;
    }

    public Coordinator getCoordinator() {
        return localAttach.getTarget() instanceof Coordinator ? (Coordinator) localAttach.getTarget() : null;
    }

    public Source getRemoteSource() {
        return remoteAttach.getSource();
    }

    public Target getRemoteTarget() {
        return remoteAttach.getTarget() instanceof Target ? (Target) remoteAttach.getTarget() : null;
    }

    public Coordinator getRemoteCoordinator() {
        return remoteAttach.getTarget() instanceof Coordinator ? (Coordinator) remoteAttach.getTarget() : null;
    }

    public boolean isRemotelyAttached() {
        return remoteAttach != null;
    }

    public boolean isRemotelyDetached() {
        return remoteDetach != null;
    }

    public boolean isLocallyAttached() {
        return localAttach != null;
    }

    public boolean isLocallyDetached() {
        return localDetach != null;
    }

    public LinkTracker handleLocalAttach(Attach localAttach) {
        this.localAttach = localAttach;

        return this;
    }

    public LinkTracker handleLocalDetach(Detach localDetach) {
        this.localDetach = localDetach;

        return this;
    }

    public void handlerRemoteAttach(Attach remoteAttach) {
        this.remoteAttach = remoteAttach;
    }

    public LinkTracker handleRemoteDetach(Detach remoteDetach) {
        this.remoteDetach = remoteDetach;

        return this;
    }

    protected abstract void handleTransfer(Transfer transfer, ByteBuffer payload);

    protected abstract void handleFlow(Flow flow);

    public abstract boolean isSender();

    public abstract boolean isReceiver();

}