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

package org.apache.qpid.protonj2.client.impl;

import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.qpid.protonj2.client.NextReceiverPolicy;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.IncomingDelivery;

/**
 * Implements the various strategies of selecting the next receiver with pending
 * messages and or waiting for new incoming messages.
 */
final class ClientNextReceiverSelector {

    private static final String LAST_RETURNED_STATE_KEY = "Last_Returned_State";

    private final ArrayDeque<ClientFuture<Receiver>> pending = new ArrayDeque<>();
    private final ClientSession session;
    private SecureRandom srand;

    public ClientNextReceiverSelector(ClientSession session) {
        this.session = session;

        // Same processing as reconnect for session delivery tapping
        handleReconnect();
    }

    public void nextReceiver(ClientFuture<Receiver> request, NextReceiverPolicy policy, long timeout) {
        Objects.requireNonNull(policy, "The next receiver selection policy cannot be null");

        ClientReceiver result = null;

        switch (policy) {
            case ROUND_ROBIN:
                result = selectNextAvailable();
                break;
            case FIRST_AVAILABLE:
                result = selectFirstAvailable();
                break;
            case LARGEST_BACKLOG:
                result = selectLargestBacklog();
                break;
            case SMALLEST_BACKLOG:
                result = selectSmallestBacklog();
                break;
            case RANDOM:
                result = selectRandomReceiver();
                break;
            default:
                request.failed(new ClientException("Next receiver called with invalid or unknown policy:" + policy));
                break;
        }

        if (result == null) {
            pending.add(request); // Wait for the next incoming delivery
            if (timeout > 0) {
                session.getScheduler().schedule(() -> {
                    if (!request.isDone()) {
                        pending.remove(request);
                        request.complete(null);
                    }
                }, timeout, TimeUnit.MILLISECONDS);
            }
        } else {
            // Track last returned to update state for Round Robin next receiver dispatch
            // this effectively ties all policies together in updating the next result from
            // a call that requests the round robin fairness policy.
            result.protonLink().getSession().getAttachments().set(LAST_RETURNED_STATE_KEY, result);

            request.complete(result);
        }
    }

    public void handleReconnect() {
        session.getProtonSession().deliveryReadHandler(this::deliveryReadHandler);
    }

    public void handleShutdown() {
        ClientException cause = null;

        if (session.isClosed()) {
            cause = new ClientIllegalStateException("The Session was explicitly closed", session.getFailureCause());
        } else if (session.getFailureCause() != null) {
            cause = session.getFailureCause();
        } else {
            cause = new ClientIllegalStateException("The session was closed without a specific error being provided");
        }

        for (ClientFuture<Receiver> request : pending) {
            request.failed(cause);
        }

        pending.clear();
    }

    private ClientReceiver selectRandomReceiver() {
        ArrayList<ClientReceiver> candidates = session.getProtonSession().receivers().stream()
                .filter((r) -> r.getLinkedResource() instanceof ClientReceiver &&
                               r.getLinkedResource(ClientReceiver.class).queuedDeliveries() > 0)
                .map((r) -> (ClientReceiver) r.getLinkedResource())
                .collect(Collectors.toCollection(ArrayList::new));

        if (srand == null) {
            srand = new SecureRandom();
        }

        Collections.shuffle(candidates, srand);

        return candidates.isEmpty() ? null : candidates.get(0);
    }

    private ClientReceiver selectNextAvailable() {
        final ClientReceiver lastReceiver = session.getProtonSession().getAttachments().get(LAST_RETURNED_STATE_KEY);

        ClientReceiver result = null;

        if (lastReceiver != null && !lastReceiver.protonReceiver.isLocallyClosedOrDetached()) {
            boolean foundLast = false;
            for (org.apache.qpid.protonj2.engine.Receiver protonReceover : session.getProtonSession().receivers()) {
                if (protonReceover.getLinkedResource() instanceof ClientReceiver) {
                    if (foundLast) {
                        ClientReceiver candidate = protonReceover.getLinkedResource();
                        if (candidate.queuedDeliveries() > 0) {
                            result = candidate;
                        }
                    } else {
                        foundLast = protonReceover.getLinkedResource() == lastReceiver;
                    }
                }
            }
        } else {
            session.getProtonSession().getAttachments().set(LAST_RETURNED_STATE_KEY, null);
        }

        return result != null ? result : selectFirstAvailable();
    }

    private ClientReceiver selectFirstAvailable() {
        return session.getProtonSession().receivers().stream()
                .filter((r) -> r.getLinkedResource() instanceof ClientReceiver &&
                               r.getLinkedResource(ClientReceiver.class).queuedDeliveries() > 0)
                .map((r) -> (ClientReceiver) r.getLinkedResource())
                .findFirst()
                .orElse(null);
    }

    private ClientReceiver selectLargestBacklog() {
        return session.getProtonSession().receivers().stream()
                .filter((r) -> r.getLinkedResource() instanceof ClientReceiver &&
                               r.getLinkedResource(ClientReceiver.class).queuedDeliveries() > 0)
                .map((r) -> (ClientReceiver) r.getLinkedResource())
                .max(Comparator.comparingLong(ClientReceiver::queuedDeliveries))
                .orElse(null);
    }

    private ClientReceiver selectSmallestBacklog() {
        return session.getProtonSession().receivers().stream()
                .filter((r) -> r.getLinkedResource() instanceof ClientReceiver &&
                               r.getLinkedResource(ClientReceiver.class).queuedDeliveries() > 0)
                .map((r) -> (ClientReceiver) r.getLinkedResource())
                .min(Comparator.comparingLong(ClientReceiver::queuedDeliveries))
                .orElse(null);
    }

    private void deliveryReadHandler(IncomingDelivery delivery) {
        // When a new delivery arrives that is completed
        if (!pending.isEmpty() && !delivery.isPartial() && !delivery.isAborted()) {
            // We only handle next receiver events for normal client receivers and
            // not for stream receiver types etc.
            if (delivery.getLink().getLinkedResource() instanceof ClientReceiver) {
                ClientReceiver receiver = delivery.getLink().getLinkedResource();

                // Track last returned to update state for Round Robin next receiver dispatch
                delivery.getLink().getSession().getAttachments().set(LAST_RETURNED_STATE_KEY, receiver);

                pending.poll().complete(receiver);
            }
        }
    }
}
