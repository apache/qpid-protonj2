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
package org.messaginghub.amqperative.util;

import org.messaginghub.amqperative.Delivery;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.impl.ClientDelivery;

/**
 * Queue based storage interface for inbound AMQP {@link Delivery} objects.
 */
public interface DeliveryQueue {

    /**
     * Adds the given {@link Delivery} to the end of the Delivery queue.
     *
     * @param delivery
     *        The in-bound Delivery to enqueue.
     */
    void enqueue(ClientDelivery delivery);

    /**
     * Adds the given {@link Delivery} to the front of the queue.
     *
     * @param delivery
     *        The in-bound Delivery to enqueue.
     */
    void enqueueFirst(ClientDelivery delivery);

    /**
     * Used to get an {@link Delivery}. The amount of time this method blocks is based on the timeout value
     * that is supplied to it.
     *
     * <ul>
     *  <li>
     *   If the timeout value is less than zero the dequeue operation blocks until a Delivery
     *   is enqueued or the queue is stopped.
     *  </li>
     *  <li>
     *   If the timeout value is zero the dequeue operation will not block and will either return
     *   the next Delivery on the Queue or null to indicate the queue is empty.
     *  </li>
     *  <li>
     *   If the timeout value is greater than zero then the method will either return the next Delivery
     *   in the queue or block until the timeout (in milliseconds) has expired or until a new Delivery
     *   is placed onto the queue.
     *  </li>
     * </ul>
     *
     * @param timeout
     *      The amount of time to wait for an entry to be added before returning null.
     *
     * @return null if we timeout or if the {@link Receiver} is closed.
     *
     * @throws InterruptedException if the wait is interrupted.
     */
    ClientDelivery dequeue(long timeout) throws InterruptedException;

    /**
     * Used to get an enqueued {@link Delivery} if on exists, otherwise returns null.
     *
     * @return the next Delivery in the Queue if one exists, otherwise null.
     */
    ClientDelivery dequeueNoWait();

    /**
     * Starts the Delivery Queue.  An non-started Queue will always return null for
     * any of the Queue methods.
     */
    void start();

    /**
     * Stops the Delivery Queue.  Deliveries cannot be read from the Queue when it is in
     * the stopped state and any waiters will be woken.
     */
    void stop();

    /**
     * Closes the Delivery Queue.  No Delivery can be added or removed from the Queue
     * once it has entered the closed state.
     */
    void close();

    /**
     * @return true if the Queue is not in the stopped or closed state.
     */
    boolean isRunning();

    /**
     * @return true if the Queue has been closed.
     */
    boolean isClosed();

    /**
     * @return true if there are no deliveries in the queue.
     */
    boolean isEmpty();

    /**
     * Returns the number of deliveries currently in the Queue.  This value is only
     * meaningful at the time of the call as the size of the Queue changes rapidly
     * as deliveries arrive and are consumed.
     *
     * @return the current number of {@link Delivery} objects in the Queue.
     */
    int size();

    /**
     * Clears the Queue of any queued {@link Delivery} values.
     */
    void clear();

}