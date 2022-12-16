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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transport.Attach;
import org.apache.qpid.protonj2.test.driver.codec.transport.Begin;
import org.apache.qpid.protonj2.test.driver.codec.transport.Detach;
import org.apache.qpid.protonj2.test.driver.codec.transport.Disposition;
import org.apache.qpid.protonj2.test.driver.codec.transport.End;
import org.apache.qpid.protonj2.test.driver.codec.transport.Flow;
import org.apache.qpid.protonj2.test.driver.codec.transport.Role;
import org.apache.qpid.protonj2.test.driver.codec.transport.Transfer;

import io.netty5.buffer.Buffer;

/**
 * Tracks information related to an opened Session and its various links
 */
public class SessionTracker {

    private final Map<String, LinkTracker> senderByNameMap = new LinkedHashMap<>();
    private final Map<String, LinkTracker> receiverByNameMap = new LinkedHashMap<>();

    private final Map<UnsignedInteger, LinkTracker> localLinks = new LinkedHashMap<>();
    private final Map<UnsignedInteger, LinkTracker> remoteLinks = new LinkedHashMap<>();

    private UnsignedShort localChannel;
    private UnsignedShort remoteChannel;
    private UnsignedInteger nextIncomingId;
    private UnsignedInteger nextOutgoingId;
    private Begin remoteBegin;
    private Begin localBegin;
    private End remoteEnd;
    private End localEnd;

    private final AMQPTestDriver driver;

    public SessionTracker(AMQPTestDriver driver) {
        this.driver = driver;
    }

    public AMQPTestDriver getDriver() {
        return driver;
    }

    public LinkTracker getLastOpenedLink() {
        final AtomicReference<LinkTracker> linkTracker = new AtomicReference<>();
        localLinks.forEach((key, value) -> {
            linkTracker.set(value);
        });

        return linkTracker.get();
    }

    public LinkTracker getLastRemotelyOpenedLink() {
        final AtomicReference<LinkTracker> linkTracker = new AtomicReference<>();
        remoteLinks.forEach((key, value) -> {
            linkTracker.set(value);
        });

        return linkTracker.get();
    }

    public LinkTracker getLastOpenedCoordinatorLink() {
        final AtomicReference<LinkTracker> linkTracker = new AtomicReference<>();
        localLinks.forEach((key, value) -> {
            if (value.getCoordinator() != null) {
                linkTracker.set(value);
            }
        });

        return linkTracker.get();
    }

    public LinkTracker getLastRemotelyOpenedCoordinatorLink() {
        final AtomicReference<LinkTracker> linkTracker = new AtomicReference<>();
        remoteLinks.forEach((key, value) -> {
            if (value.getRemoteCoordinator() != null) {
                linkTracker.set(value);
            }
        });

        return linkTracker.get();
    }

    public LinkTracker getLastOpenedRemoteSender() {
        final AtomicReference<LinkTracker> linkTracker = new AtomicReference<>();
        remoteLinks.forEach((key, value) -> {
            if (value.isReceiver()) {
                linkTracker.set(value);
            }
        });

        return linkTracker.get();
    }

    public LinkTracker getLastOpenedRemoteReceiver() {
        final AtomicReference<LinkTracker> linkTracker = new AtomicReference<>();
        remoteLinks.forEach((key, value) -> {
            if (value.isSender()) {
                linkTracker.set(value);
            }
        });

        return linkTracker.get();
    }

    public LinkTracker getLastOpenedSender() {
        final AtomicReference<LinkTracker> linkTracker = new AtomicReference<>();
        localLinks.forEach((key, value) -> {
            if (value.isSender()) {
                linkTracker.set(value);
            }
        });

        return linkTracker.get();
    }

    public LinkTracker getLastOpenedReceiver() {
        final AtomicReference<LinkTracker> linkTracker = new AtomicReference<>();
        localLinks.forEach((key, value) -> {
            if (value.isReceiver()) {
                linkTracker.set(value);
            }
        });

        return linkTracker.get();
    }

    //----- Session specific access which can provide details for expectations

    public End getRemoteEnd() {
        return remoteEnd;
    }

    public End getLocalEnd() {
        return localEnd;
    }

    public Begin getRemoteBegin() {
        return remoteBegin;
    }

    public Begin getLocalBegin() {
        return localBegin;
    }

    public UnsignedShort getRemoteChannel() {
        return remoteChannel;
    }

    public UnsignedShort getLocalChannel() {
        return localChannel;
    }

    public UnsignedInteger getNextIncomingId() {
        return nextIncomingId;
    }

    public UnsignedInteger getNextOutgoingId() {
        return nextOutgoingId;
    }

    //----- Handle performatives and update session state

    public SessionTracker handleBegin(Begin remoteBegin, UnsignedShort remoteChannel) {
        this.remoteBegin = remoteBegin;
        this.remoteChannel = remoteChannel;
        this.nextIncomingId = remoteBegin.getNextOutgoingId();

        return this;
    }

    public SessionTracker handleLocalBegin(Begin localBegin, UnsignedShort localChannel) {
        this.localBegin = localBegin;
        this.localChannel = localChannel;
        this.nextOutgoingId = localBegin.getNextOutgoingId();

        return this;
    }

    public SessionTracker handleEnd(End end) {
        this.remoteEnd = end;
        return this;
    }

    public SessionTracker handleLocalEnd(End end) {
        this.localEnd = end;
        return this;
    }

    public LinkTracker handleRemoteAttach(Attach attach) {
        LinkTracker linkTracker = remoteLinks.get(attach.getHandle());

        if (linkTracker != null) {
            throw new AssertionError(String.format(
                "Received second attach of link handle %s with name %s", attach.getHandle(), attach.getName()));
        }

        final UnsignedInteger localHandleMax = localBegin == null ? UnsignedInteger.ZERO :
            localBegin.getHandleMax() == null ? UnsignedInteger.MAX_VALUE : localBegin.getHandleMax();

        if (attach.getHandle().compareTo(localHandleMax) > 0) {
            throw new AssertionError("Session Handle Max [" + localHandleMax + "] Exceeded for link Attach: " + attach.getHandle());
        }

        // Check that the remote attach is an original link attach with no corresponding local
        // attach having already been done or not as there should only ever be one instance of
        // a link tracker for any given link.
        linkTracker = findMatchingPendingLinkOpen(attach);
        if (linkTracker == null) {
            if (attach.getRole().equals(Role.SENDER.getValue())) {
                linkTracker = new ReceiverTracker(this);
                receiverByNameMap.put(attach.getName(), linkTracker);
            } else {
                linkTracker = new SenderTracker(this);
                senderByNameMap.put(attach.getName(), linkTracker);
            }
        }

        remoteLinks.put(attach.getHandle(), linkTracker);
        linkTracker.handlerRemoteAttach(attach);

        if (linkTracker.getRemoteCoordinator() != null) {
            getDriver().sessions().setLastOpenedCoordinator(linkTracker);
        }

        return linkTracker;
    }

    public LinkTracker handleLocalAttach(Attach attach) {
        LinkTracker linkTracker = localLinks.get(attach.getHandle());

        // Create a tracker for the local side to use to respond to remote
        // performative or to use when invoking local actions but don't validate
        // that it was already sent one as a test might be checking remote handling.
        if (linkTracker == null) {
            if (attach.getRole().equals(Role.SENDER.getValue())) {
                linkTracker = senderByNameMap.get(attach.getName());
                if (linkTracker == null) {
                    linkTracker = new SenderTracker(this);
                    senderByNameMap.put(attach.getName(), linkTracker);
                }
            } else {
                linkTracker = receiverByNameMap.get(attach.getName());
                if (linkTracker == null) {
                    linkTracker = new ReceiverTracker(this);
                    receiverByNameMap.put(attach.getName(), linkTracker);
                }
            }

            localLinks.put(attach.getHandle(), linkTracker);
            linkTracker.handleLocalAttach(attach);
        }

        return linkTracker;
    }

    public LinkTracker handleRemoteDetach(Detach detach) {
        LinkTracker tracker = remoteLinks.get(detach.getHandle());

        if (tracker != null) {
            tracker.handleRemoteDetach(detach);
            remoteLinks.remove(detach.getHandle());

            if (tracker.isLocallyDetached()) {
                if (tracker.isSender()) {
                    senderByNameMap.remove(tracker.getName());
                } else {
                    receiverByNameMap.remove(tracker.getName());
                }
            }
        } else {
            throw new AssertionError(String.format(
                "Received Detach for unknown remote link with handle %s", detach.getHandle()));
        }

        return tracker;
    }

    public LinkTracker handleLocalDetach(Detach detach) {
        LinkTracker tracker = localLinks.get(detach.getHandle());

        // Handle the detach and remove if we knew about it, otherwise ignore as
        // the test might be checked for handling of unexpected End frames etc.
        if (tracker != null) {
            tracker.handleLocalDetach(detach);
            localLinks.remove(detach.getHandle());

            if (tracker.isRemotelyDetached()) {
                if (tracker.isSender()) {
                    senderByNameMap.remove(tracker.getName());
                } else {
                    receiverByNameMap.remove(tracker.getName());
                }
            }
        }

        return tracker;
    }

    public LinkTracker handleTransfer(Transfer transfer, Buffer payload) {
        LinkTracker tracker = remoteLinks.get(transfer.getHandle());

        if (tracker.isSender()) {
            throw new AssertionError("Received inbound Transfer addressed to a local Sender link");
        } else {
            tracker.handleTransfer(transfer, payload);
            // TODO - Update session state based on transfer
        }

        return tracker;
    }

    public void handleLocalTransfer(Transfer transfer, Buffer payload) {
        LinkTracker tracker = localLinks.get(transfer.getHandle());

        // Pass along to local sender for processing before sending and ignore if
        // we aren't tracking a link or the link is a receiver as the test might
        // be checking how the remote handles invalid frames.
        if (tracker != null && tracker.isSender()) {
            tracker.handleTransfer(transfer, payload);
            // TODO - Update session state based on transfer
        }
    }

    public void handleDisposition(Disposition disposition) {
        // TODO Forward to attached links or issue errors if invalid.
    }

    public void handleLocalDisposition(Disposition disposition) {
        // TODO Forward to attached links or issue error if invalid.
    }

    public LinkTracker handleFlow(Flow flow) {
        LinkTracker tracker = null;

        if (flow.getHandle() != null) {
            tracker = remoteLinks.get(flow.getHandle());
            if (tracker != null) {
                tracker.handleFlow(flow);
            } else {
                throw new AssertionError(String.format(
                    "Received Flow for unknown remote link with handle %s", flow.getHandle()));
            }
        }

        return tracker;
    }

    public UnsignedInteger findFreeLocalHandle() {
        final UnsignedInteger HANDLE_MAX = localBegin.getHandleMax() != null ? localBegin.getHandleMax() : UnsignedInteger.MAX_VALUE;

        for (long i = 0; i <= HANDLE_MAX.longValue(); ++i) {
            final UnsignedInteger handle = UnsignedInteger.valueOf(i);
            if (!localLinks.containsKey(handle)) {
                return handle;
            }
        }

        throw new IllegalStateException("no local handle available for allocation");
    }

    private LinkTracker findMatchingPendingLinkOpen(Attach remoteAttach) {
        for (LinkTracker link : senderByNameMap.values()) {
            if (link.getName().equals(remoteAttach.getName()) &&
                !link.isRemotelyAttached() &&
                remoteAttach.isReceiver()) {

                return link;
            }
        }

        for (LinkTracker link : receiverByNameMap.values()) {
            if (link.getName().equals(remoteAttach.getName()) &&
                !link.isRemotelyAttached() &&
                remoteAttach.isSender()) {

                return link;
            }
        }

        return null;
    }
}
