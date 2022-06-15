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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.impl.ClientDelivery;

/**
 * Simple first in / first out {@link Delivery} Queue.
 */
public final class FifoDeliveryQueue implements DeliveryQueue {

    private static final AtomicIntegerFieldUpdater<FifoDeliveryQueue> STATE_FIELD_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(FifoDeliveryQueue.class, "state");

    private static final int CLOSED = 0;
    private static final int STOPPED = 1;
    private static final int RUNNING = 2;

    private volatile int state = STOPPED;

    private int waiters = 0;

    private final Deque<ClientDelivery> queue;

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
    public void enqueueFirst(ClientDelivery envelope) {
        synchronized (queue) {
            queue.addFirst(envelope);
            if (waiters > 0) {
                queue.notify();
            }
        }
    }

    @Override
    public void enqueue(ClientDelivery envelope) {
        synchronized (queue) {
            queue.addLast(envelope);
            if (waiters > 0) {
                queue.notify();
            }
        }
    }

    @Override
    public ClientDelivery dequeue(long timeout) throws InterruptedException {
        synchronized (queue) {
            // Wait until the receiver is ready to deliver messages.
            while (timeout != 0 && isRunning() && queue.isEmpty()) {
                if (timeout == -1) {
                    waiters++;
                    try {
                        queue.wait();
                    } finally {
                        waiters--;
                    }
                } else {
                    long start = System.currentTimeMillis();
                    waiters++;
                    try {
                        queue.wait(timeout);
                    } finally {
                        waiters--;
                    }
                    timeout = Math.max(timeout + start - System.currentTimeMillis(), 0);
                }
            }

            if (!isRunning()) {
                return null;
            }

            return queue.pollFirst();
        }
    }

    @Override
    public ClientDelivery dequeueNoWait() {
        synchronized (queue) {
            if (!isRunning()) {
                return null;
            }

            return queue.pollFirst();
        }
    }

    @Override
    public void start() {
        if (STATE_FIELD_UPDATER.compareAndSet(this, STOPPED, RUNNING)) {
            synchronized (queue) {
                if (waiters > 0) {
                    queue.notifyAll();
                }
            }
        }
    }

    @Override
    public void stop() {
        if (STATE_FIELD_UPDATER.compareAndSet(this, RUNNING, STOPPED)) {
            synchronized (queue) {
                if (waiters > 0) {
                    queue.notifyAll();
                }
            }
        }
    }

    @Override
    public void close() {
        if (STATE_FIELD_UPDATER.getAndSet(this, CLOSED) > CLOSED) {
            synchronized (queue) {
                if (waiters > 0) {
                    queue.notifyAll();
                }
            }
        }
    }

    @Override
    public boolean isRunning() {
        return state == RUNNING;
    }

    @Override
    public boolean isClosed() {
        return state == CLOSED;
    }

    @Override
    public boolean isEmpty() {
        synchronized (queue) {
            return queue.isEmpty();
        }
    }

    @Override
    public int size() {
        synchronized (queue) {
            return queue.size();
        }
    }

    @Override
    public void clear() {
        synchronized (queue) {
            queue.clear();
        }
    }

    @Override
    public String toString() {
        synchronized (queue) {
            return queue.toString();
        }
    }
}
