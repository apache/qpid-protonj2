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
import java.util.function.Supplier;

import org.apache.qpid.protonj2.client.exceptions.ClientSendTimedOutException;
import org.apache.qpid.protonj2.engine.DeliveryTagGenerator;

/**
 * Options that control the behavior of a {@link Sender} created from them.
 */
public class SenderOptions extends LinkOptions<SenderOptions> implements Cloneable {

    private long sendTimeout = ConnectionOptions.DEFAULT_SEND_TIMEOUT;

    private Supplier<DeliveryTagGenerator> tagGeneratorSupplier;

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
     * @return the {@link SenderOptions} instance that was given.
     */
    protected SenderOptions copyInto(SenderOptions other) {
        super.copyInto(other);

        other.sendTimeout(sendTimeout);
        other.deliveryTagGeneratorSupplier(tagGeneratorSupplier);

        return other;
    }

    /**
     * Configures a {@link Supplier} which provides unique instances of {@link DeliveryTagGenerator} objects
     * for any {@link Sender} created using these options.
     * <p>
     * The client sender will use a default {@link DeliveryTagGenerator} under normal circumstances and the
     * user is not required to configure a {@link Supplier}. In some cases where the user is communicating
     * with a system that requires a specific format of delivery tag this option allows use of a custom
     * generator. The caller is responsible for providing a supplier that will create a unique instance of
     * a tag generator as they are not meant to be shared amongst senders. Once a sender has been created
     * the tag generator it uses cannot be changed so future calls to this method will not affect previously
     * created {@link Sender} instances.
     *
     * @param supplier
     * 		The {@link Supplier} of {@link DeliveryTagGenerator} instances.
     *
     * @return the {@link SenderOptions} instance that was given.
     */
    public SenderOptions deliveryTagGeneratorSupplier(Supplier<DeliveryTagGenerator> supplier) {
        this.tagGeneratorSupplier = supplier;
        return this;
    }

    /**
     * @return the configured delivery tag {@link Supplier} or null if none was set.
     */
    public Supplier<DeliveryTagGenerator> deliveryTagGeneratorSupplier() {
        return tagGeneratorSupplier;
    }

    @Override
    protected SenderOptions self() {
        return this;
    }
}
