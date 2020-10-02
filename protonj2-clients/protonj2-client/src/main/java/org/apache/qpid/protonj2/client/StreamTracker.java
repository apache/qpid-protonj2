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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.exceptions.ClientException;

/**
 * Special StreamSender related {@link Tracker} that is linked to any {@link StreamSenderMessage}
 * instance and provides the {@link Tracker} functions for those types of messages.
 */
public interface StreamTracker extends Tracker {

    /**
     * {@inheritDoc}
     *
     * @return the {@link StreamSender} that is associated with this {@link StreamTracker}.
     */
    @Override
    StreamSender sender();

    /**
     * {@inheritDoc}
     *
     * @return this {@link StreamTracker} instance.
     */
    @Override
    StreamTracker settle() throws ClientException;

    /**
     * {@inheritDoc}
     */
    @Override
    Future<Tracker> settlementFuture();

    /**
     * {@inheritDoc}
     *
     * @return this {@link StreamTracker} instance.
     */
    @Override
    StreamTracker disposition(DeliveryState state, boolean settle) throws ClientException;

    /**
     * {@inheritDoc}
     *
     * @return this {@link StreamTracker} instance.
     */
    @Override
    StreamTracker awaitSettlement() throws ClientException;

    /**
     * {@inheritDoc}
     *
     * @return this {@link StreamTracker} instance.
     */
    @Override
    StreamTracker awaitSettlement(long timeout, TimeUnit unit) throws ClientException;

}
