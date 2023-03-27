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
 * AMQP Receiver that provides an interface to receive complete Deliveries from a remote
 * peer.  Deliveries that are returned from the {@link #receive()} methods will be complete
 * and can be read immediately without blocking waiting for additional delivery data to arrive.
 *
 * @see StreamReceiver
 */
public interface Receiver extends Link<Receiver> {

    /**
     * Adds credit to the {@link Receiver} link for use when there receiver has not been configured
     * with a credit window.  When credit window is configured credit replenishment is automatic and
     * calling this method will result in an exception indicating that the operation is invalid.
     * <p>
     * If the {@link Receiver} is draining and this method is called an exception will be thrown
     * to indicate that credit cannot be replenished until the remote has drained the existing link
     * credit.
     *
     * @param credits
     *      The number of credits to add to the {@link Receiver} link.
     *
     * @return this {@link Receiver} instance.
     *
     * @throws ClientException if an error occurs while attempting to add new {@link Receiver} link credit.
     */
    Receiver addCredit(int credits) throws ClientException;

    /**
     * Blocking receive method that waits forever for the remote to provide a {@link Delivery} for consumption.
     * <p>
     * Receive calls will only grant credit on their own if a credit window is configured in the
     * {@link ReceiverOptions} which is done by default.  If the client application has configured
     * no credit window than this method will not grant any credit when it enters the wait for new
     * incoming messages.
     *
     * @return a new {@link Delivery} received from the remote.
     *
     * @throws ClientException if the {@link Receiver} or its parent is closed when the call to receive is made.
     */
    Delivery receive() throws ClientException;

    /**
     * Blocking receive method that waits the given time interval for the remote to provide a
     * {@link Delivery} for consumption. The amount of time this method blocks is based on the
     * timeout value. If the timeout is less than zero then it blocks until a Delivery is received.
     * If the timeout is equal to zero then it will not block and simply return a {@link Delivery}
     * if one is available locally. If the timeout value is greater than zero then it blocks up to
     * timeout amount of time.
     * <p>
     * Receive calls will only grant credit on their own if a credit window is configured in the
     * {@link ReceiverOptions} which is done by default. If the client application has not configured
     * a credit window or granted credit manually this method will not automatically grant any credit
     * when it enters the wait for a new incoming {@link Delivery}.
     *
     * @param timeout
     *      The timeout value used to control how long the receive method waits for a new {@link Delivery}.
     * @param unit
     *      The unit of time that the given timeout represents.
     *
     * @return a new {@link Delivery} received from the remote.
     *
     * @throws ClientException if the {@link Receiver} or its parent is closed when the call to receive is made.
     */
    Delivery receive(long timeout, TimeUnit unit) throws ClientException;

    /**
     * Non-blocking receive method that either returns a message is one is immediately available or
     * returns null if none is currently at hand.
     *
     * @return a new {@link Delivery} received from the remote or null if no pending deliveries are available.
     *
     * @throws ClientException if the {@link Receiver} or its parent is closed when the call to try to receive is made.
     */
    Delivery tryReceive() throws ClientException;

    /**
     * Requests the remote to drain previously granted credit for this {@link Receiver} link.
     *
     * @return a {@link Future} that will be completed when the remote drains this {@link Receiver} link.
     *
     * @throws ClientException if an error occurs while attempting to drain the link credit.
     */
    Future<Receiver> drain() throws ClientException;

    /**
     * Returns the number of Deliveries that are currently held in the {@link Receiver} delivery
     * queue.  This number is likely to change immediately following the call as more deliveries
     * arrive but can be used to determine if any pending {@link Delivery} work is ready.
     *
     * @return the number of deliveries that are currently buffered locally.
     *
     * @throws ClientException if an error occurs while attempting to fetch the queue count.
     */
    long queuedDeliveries() throws ClientException;

}
