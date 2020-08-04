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

import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.MessageOutputStream;
import org.apache.qpid.protonj2.client.MessageOutputStreamOptions;
import org.apache.qpid.protonj2.client.RawOutputStream;
import org.apache.qpid.protonj2.client.RawOutputStreamOptions;
import org.apache.qpid.protonj2.client.SendContext;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

/**
 * Sender Context used to multiple send operations that comprise the payload
 * of a single delivery.
 */
public class ClientSendContext implements SendContext {

    private final ClientSender sender;
    private ClientTracker tracker;

    private volatile boolean completed;
    private volatile boolean aborted;
    private volatile int messageFormat;

    ClientSendContext(ClientSender sender) {
        this.sender = sender;
    }

    int messageFormat() {
        return messageFormat;
    }

    ClientSendContext tracker(ClientTracker tracker) {
        this.tracker = tracker;
        return this;
    }

    @Override
    public ClientTracker tracker() {
        return tracker;
    }

    @Override
    public Tracker abort() throws ClientException {
        if (completed()) {
            throw new IllegalStateException("Cannot abort an already completed send context");
        }

        if (!aborted()) {
            aborted = true;

            if (tracker != null) {
                sender.abort(tracker.delivery());
            }
        }

        return tracker();
    }

    @Override
    public boolean aborted() {
        return aborted;
    }

    @Override
    public Tracker complete() throws ClientException {
        if (aborted()) {
            throw new IllegalStateException("Cannot complete an already aborted send context");
        }

        if (!completed()) {
            aborted = true;

            if (tracker != null) {
                sender.complete(tracker.delivery());
            }
        }

        return tracker();
    }

    @Override
    public <E> SendContext send(AdvancedMessage<E> message) throws ClientException {
        if (aborted()) {
            throw new IllegalStateException("Cannot send from an already aborted send context");
        }

        if (completed()) {
            throw new IllegalStateException("Cannot send from an already completed send context");
        }

        if (tracker == null) {
            messageFormat = message.messageFormat();
        }

        tracker = sender.sendMessage(this, message);

        return this;
    }

    @Override
    public <E> Tracker complete(AdvancedMessage<E> message) throws ClientException {
        if (aborted()) {
            throw new IllegalStateException("Cannot send from an already aborted send context");
        }

        if (completed()) {
            throw new IllegalStateException("Cannot send from an already completed send context");
        }

        if (tracker == null) {
            messageFormat = message.messageFormat();
        }

        completed = true;

        tracker = sender.sendMessage(this, message);

        return tracker();
    }

    @Override
    public boolean completed() {
        return completed;
    }

    @Override
    public MessageOutputStream outputStream(MessageOutputStreamOptions options) {
        if (completed()) {
            throw new IllegalStateException("Cannot create an OutputStream from a completed send context");
        }

        return null;
    }

    @Override
    public RawOutputStream outputStream(RawOutputStreamOptions options) {
        if (completed()) {
            throw new IllegalStateException("Cannot create an OutputStream from a completed send context");
        }

        return null;
    }
}
