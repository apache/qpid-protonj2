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
package org.apache.qpid.protonj2.client;

import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.exceptions.ClientSendTimedOutException;

/**
 * Options that control the behavior of a {@link Sender} created from them.
 */
public class SenderOptions extends LinkOptions<SenderOptions>{

    private long sendTimeout = ConnectionOptions.DEFAULT_SEND_TIMEOUT;

    /**
     * Create a new {@link SenderOptions} instance configured with default configuration settings.
     */
    public SenderOptions() {
    }

    /**
     * Create a new SenderOptions instance that copies the configuration from the specified source options.
     *
     * @param options
     * 		The SenderOptions instance whose settings are to be copied into this one.
     */
    public SenderOptions(SenderOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    /**
     * @return the timeout used when awaiting a response from the remote when a resource is message send.
     */
    public long sendTimeout() {
        return sendTimeout;
    }

    /**
     * Configures the timeout used when awaiting a send operation to complete.  A send will block if the
     * remote has not granted the {@link Sender} or the {@link Session} credit to do so, if the send blocks
     * for longer than this timeout the send call will fail with an {@link ClientSendTimedOutException}
     * exception to indicate that the send did not complete.
     *
     * @param sendTimeout
     *      Timeout value in milliseconds to wait for a remote response.
     *
     * @return this {@link SenderOptions} instance.
     */
    public SenderOptions sendTimeout(long sendTimeout) {
        return sendTimeout(sendTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Configures the timeout used when awaiting a send operation to complete.  A send will block if the
     * remote has not granted the {@link Sender} or the {@link Session} credit to do so, if the send blocks
     * for longer than this timeout the send call will fail with an {@link ClientSendTimedOutException}
     * exception to indicate that the send did not complete.
     *
     * @param timeout
     *      Timeout value to wait for a remote response.
     * @param units
     * 		The {@link TimeUnit} that defines the timeout span.
     *
     * @return this {@link SenderOptions} instance.
     */
    public SenderOptions sendTimeout(long timeout, TimeUnit units) {
        this.sendTimeout = units.toMillis(timeout);
        return this;
    }

    @Override
    public SenderOptions clone() {
        return copyInto(new SenderOptions());
    }

    /**
     * Copy all options from this {@link SenderOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this options class for chaining.
     */
    protected SenderOptions copyInto(SenderOptions other) {
        super.copyInto(other);

        other.sendTimeout(sendTimeout);

        return this;
    }

    @Override
    protected SenderOptions self() {
        return this;
    }
}
