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
package org.apache.qpid.protonj2.client.util;

import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.impl.ClientDelivery;

/**
 * Simple first in / first out {@link Delivery} Queue.
 */
public final class FifoDeliveryQueue implements DeliveryQueue {

    private final Deque<ClientDelivery> queue;

    private volatile boolean started;
    private int waiters = 0;

    /**
     * Creates a new first in / first out message queue with the given queue depth
     *
     * @param queueDepth
     * 		The Queue depth to configure for this FIFO Message Queue.
     */
    public FifoDeliveryQueue(int queueDepth) {
        this.queue = new ArrayDeque<ClientDelivery>(Math.max(1, queueDepth));
    }

    @Override
    public synchronized void enqueue(ClientDelivery envelope) {
        queue.addLast(envelope);
        if (waiters > 0) {
            notify();
        }
    }

    @Override
    public synchronized ClientDelivery dequeue(long timeout) throws InterruptedException {
        // Wait until the receiver is ready to deliver messages.
        while (queue.isEmpty() && timeout != 0 && started) {
            if (timeout == -1) {
                waiters++;
                try {
                    wait();
                } finally {
                    waiters--;
                }
            } else {
                long start = System.currentTimeMillis();
                waiters++;
                try {
                    wait(timeout);
                } finally {
                    waiters--;
                }
                timeout = Math.max(timeout + start - System.currentTimeMillis(), 0);
            }
        }

        if (started) {
            return queue.pollFirst();
        } else {
            return null;
        }
    }

    @Override
    public synchronized ClientDelivery dequeueNoWait() {
        if (started) {
            return queue.pollFirst();
        } else {
            return null;
        }
    }

    @Override
    public synchronized void start() {
        if (!started) {
            started = true;

            if (waiters > 0) {
                notifyAll();
            }
        }
    }

    @Override
    public synchronized void stop() {
        if (started) {
            started = false;

            if (waiters > 0) {
                notifyAll();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return started;
    }

    @Override
    public synchronized boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public synchronized int size() {
        return queue.size();
    }

    @Override
    public synchronized void clear() {
        queue.clear();
    }

    @Override
    public synchronized String toString() {
        return queue.toString();
    }
}
