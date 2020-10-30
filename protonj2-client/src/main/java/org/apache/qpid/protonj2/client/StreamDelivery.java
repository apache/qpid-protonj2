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

import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * A specialized {@link Delivery} type that is returned from the {@link StreamReceiver}
 * which can be used to read incoming large messages that are streamed via multiple incoming
 * AMQP {@link Transfer} frames.
 */
public interface StreamDelivery extends Delivery {

    /**
     * @return the {@link StreamReceiver} that originated this {@link StreamDelivery}.
     */
    @Override
    StreamReceiver receiver();

    /**
     * {@inheritDoc}
     *
     * @return a {@link StreamReceiverMessage} instance that can be used to read the incoming message stream.
     */
    @SuppressWarnings("unchecked")
    @Override
    StreamReceiverMessage message() throws ClientException;

    /**
     * Check if the {@link StreamDelivery} has been marked as aborted by the remote sender.
     *
     * @return true if this context has been marked as aborted previously.
     */
    boolean aborted();

    /**
     * Check if the {@link StreamDelivery} has been marked as complete by the remote sender.
     *
     * @return true if this context has been marked as being the complete.
     */
    boolean completed();

    /**
     * {@inheritDoc}
     *
     * @return the {@link StreamReceiver} that originated this {@link StreamDelivery}.
     */
    @Override
    StreamDelivery accept() throws ClientException;

    /**
     * {@inheritDoc}
     *
     * @return the {@link StreamReceiver} that originated this {@link StreamDelivery}.
     */
    @Override
    StreamDelivery release() throws ClientException;

    /**
     * {@inheritDoc}
     *
     * @return the {@link StreamReceiver} that originated this {@link StreamDelivery}.
     */
    @Override
    StreamDelivery reject(String condition, String description) throws ClientException;

    /**
     * {@inheritDoc}
     *
     * @return the {@link StreamReceiver} that originated this {@link StreamDelivery}.
     */
    @Override
    StreamDelivery modified(boolean deliveryFailed, boolean undeliverableHere) throws ClientException;

    /**
     * {@inheritDoc}
     *
     * @return the {@link StreamReceiver} that originated this {@link StreamDelivery}.
     */
    @Override
    StreamDelivery disposition(DeliveryState state, boolean settle) throws ClientException;

    /**
     * {@inheritDoc}
     *
     * @return the {@link StreamReceiver} that originated this {@link StreamDelivery}.
     */
    @Override
    StreamDelivery settle() throws ClientException;

}
