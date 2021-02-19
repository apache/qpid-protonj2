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

import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transport.Begin;
import org.apache.qpid.protonj2.test.driver.codec.transport.End;

/**
 * Tracks all sessions opened by the remote or initiated from the driver.
 */
public class DriverSessions {

    public static final int DRIVER_DEFAULT_CHANNEL_MAX = 65535;

    private final Map<UnsignedShort, SessionTracker> localSessions = new LinkedHashMap<>();
    private final Map<UnsignedShort, SessionTracker> remoteSessions = new LinkedHashMap<>();

    private final AMQPTestDriver driver;

    private UnsignedShort lastRemotelyOpenedSession = null;
    private UnsignedShort lastLocallyOpenedSession = null;
    private LinkTracker lastCoordinator;

    public DriverSessions(AMQPTestDriver driver) {
        this.driver = driver;
    }

    public SessionTracker getLastRemotelyOpenedSession() {
        return localSessions.get(lastRemotelyOpenedSession);
    }

    public SessionTracker getLastLocallyOpenedSession() {
        return localSessions.get(lastLocallyOpenedSession);
    }

    public LinkTracker getLastOpenedCoordinator() {
        return lastCoordinator;
    }

    void setLastOpenedCoordinator(LinkTracker lastOpenedCoordinatorLink) {
        this.lastCoordinator = lastOpenedCoordinatorLink;
    }

    public AMQPTestDriver getDriver() {
        return driver;
    }

    public SessionTracker getSessionFromLocalChannel(UnsignedShort localChannel) {
        return localSessions.get(localChannel);
    }

    public SessionTracker getSessionFromRemoteChannel(UnsignedShort remoteChannel) {
        return remoteSessions.get(remoteChannel);
    }

    //----- Process performatives that require session level tracking

    public SessionTracker handleBegin(Begin remoteBegin, UnsignedShort remoteChannel) {
        if (remoteSessions.containsKey(remoteChannel)) {
            throw new AssertionError("Received duplicate Begin for already opened session on channel: " + remoteChannel);
        }

        final SessionTracker sessionTracker;  // Result that we need to update here once validation is complete.

        if (remoteBegin.getRemoteChannel() != null) {
            // This should be a response to previous Begin that this test driver sent if there
            // is a remote channel set in which case a local session should already have been
            // created and if not that is an error
            sessionTracker = localSessions.get(remoteBegin.getRemoteChannel());
            if (sessionTracker == null) {
                throw new AssertionError(String.format(
                    "Received Begin on channel [%d] that indicated it was a response to a Begin this driver never sent to channel [%d]: ",
                    remoteChannel, remoteBegin.getRemoteChannel()));
            }
        } else {
            // Remote has requested that the driver create a new session which will require a scripted
            // response in order to complete the begin cycle.  Start tracking now for future
            sessionTracker = new SessionTracker(driver);

            localSessions.put(sessionTracker.getLocalChannel(), sessionTracker);
        }

        sessionTracker.handleBegin(remoteBegin, remoteChannel);

        remoteSessions.put(remoteChannel, sessionTracker);
        lastRemotelyOpenedSession = sessionTracker.getLocalChannel();

        return sessionTracker;
    }

    public SessionTracker handleEnd(End remoteEnd, UnsignedShort remoteChannel) {
        SessionTracker sessionTracker = remoteSessions.get(remoteChannel);

        if (sessionTracker == null) {
            throw new AssertionError(String.format(
                "Received End on channel [%d] that has no matching Session for that remote channel. ", remoteChannel));
        } else {
            sessionTracker.handleEnd(remoteEnd);
            remoteSessions.remove(remoteChannel);

            return sessionTracker;
        }
    }

    //----- Process Session Begin and End from their injection actions and update state

    public SessionTracker handleLocalBegin(Begin localBegin, UnsignedShort localChannel) {
        // Are we responding to a remote Begin?  If so then we already have a SessionTracker
        // that should be correlated with the local tracker stored now that we are responding
        // to, although a test might be fiddling with unexpected Begin commands so we don't
        // assume there absolutely must be a remote session in the tracking map.
        if (localBegin.getRemoteChannel() != null && remoteSessions.containsKey(localBegin.getRemoteChannel())) {
            localSessions.put(localChannel, remoteSessions.get(localBegin.getRemoteChannel()));
        }

        if (!localSessions.containsKey(localChannel)) {
            localSessions.put(localChannel, new SessionTracker(driver));
        }

        lastLocallyOpenedSession = localChannel;

        return localSessions.get(localChannel).handleLocalBegin(localBegin, localChannel);
    }

    public SessionTracker handleLocalEnd(End localEnd, UnsignedShort localChannel) {
        // A test script might trigger multiple end calls or otherwise mess with normal
        // AMQP processing no in case we can't find it, just return a dummy that the
        // script can use.
        if (localSessions.containsKey(localChannel)) {
            return localSessions.get(localChannel).handleLocalEnd(localEnd);
        } else {
            return new SessionTracker(driver).handleLocalEnd(localEnd);
        }
    }

    //----- Driver Session Management API

    public int findFreeLocalChannel() {
        // TODO: Respect local channel max if one was set on open.
        for (int i = 0; i <= DRIVER_DEFAULT_CHANNEL_MAX; ++i) {
            if (!localSessions.containsKey(UnsignedShort.valueOf(i))) {
                return i;
            }
        }

        throw new IllegalStateException("no local channel available for allocation");
    }

    void freeLocalChannel(UnsignedShort localChannel) {
        localSessions.remove(localChannel);
    }
}
