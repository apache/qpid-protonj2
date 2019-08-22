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
package org.messaginghub.amqperative.futures;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A more conservative implementation of a ClientFuture that is better on some
 * platforms or resource constrained hardware where high CPU usage can be more
 * counter productive than other variants that might spin or otherwise avoid
 * entry into states requiring thread signaling.
 *
  * @param <V> The type that result from completion of this Future
*/
public class ConservativeClientFuture<V> extends ClientFuture<V> {

    public ConservativeClientFuture() {
        this(null);
    }

    public ConservativeClientFuture(ClientSynchronization synchronization) {
        super(synchronization);
    }

    @Override
    public V get(long amount, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            if (isComplete() || amount == 0) {
                failOnError();
                return getResult();
            }

            final long timeout = unit.toNanos(amount);
            long maxParkNanos = timeout / 8;
            maxParkNanos = maxParkNanos > 0 ? maxParkNanos : timeout;
            final long startTime = System.nanoTime();

            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }

            while (true) {
                final long elapsed = System.nanoTime() - startTime;
                final long diff = elapsed - timeout;

                if (diff >= 0) {
                    failOnError();
                    if (isComplete()) {
                        return getResult();
                    }
                    throw new TimeoutException("Timed out waiting for completion");
                }

                if (isComplete()) {
                    failOnError();
                    return getResult();
                }

                synchronized (this) {
                    if (isComplete()) {
                        failOnError();
                        return getResult();
                    }

                    waiting++;
                    try {
                        wait(-diff / 1000000, (int) (-diff % 1000000));
                    } finally {
                        waiting--;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw e;
        }
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        try {
            if (isComplete()) {
                failOnError();
                return getResult();
            }

            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }

            while (true) {
                if (isComplete()) {
                    failOnError();
                    return getResult();
                }

                synchronized (this) {
                    if (isComplete()) {
                        failOnError();
                        return getResult();
                    }

                    waiting++;
                    try {
                        wait();
                    } finally {
                        waiting--;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw e;
        }
    }
}
