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
package org.apache.qpid.protonj2.client.futures;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.security.ProviderException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(20)
public class ClientFutureTest {

    @ParameterizedTest
    @ValueSource(strings = { "conservative", "balanced", "progressive" })
    public void testIsComplete(String futureType) {
        final ClientFutureFactory futuresFactory = ClientFutureFactory.create(futureType);
        final ClientFuture<Void> future = futuresFactory.createFuture();

        assertFalse(future.isComplete());
        future.complete(null);
        assertTrue(future.isComplete());
    }

    @ParameterizedTest
    @ValueSource(strings = { "conservative", "balanced", "progressive" })
    public void testOnSuccess(String futureType) {
        final ClientFutureFactory futuresFactory = ClientFutureFactory.create(futureType);
        final ClientFuture<Void> future = futuresFactory.createFuture();

        future.complete(null);
        try {
            future.get();
        } catch (Exception cause) {
            fail("Should not throw an error");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "conservative", "balanced", "progressive" })
    public void testTimedGet(String futureType) {
        final ClientFutureFactory futuresFactory = ClientFutureFactory.create(futureType);
        final ClientFuture<Void> future = futuresFactory.createFuture();

        try {
            assertNull(future.get(500, TimeUnit.MILLISECONDS));
        } catch (TimeoutException cause) {
        } catch (Exception cause) {
            fail("Should not throw an error");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "conservative", "balanced", "progressive" })
    public void testTimedGetWhenComplete(String futureType) {
        final ClientFutureFactory futuresFactory = ClientFutureFactory.create(futureType);
        final ClientFuture<Void> future = futuresFactory.createFuture();

        future.complete(null);

        try {
            assertNull(future.get(500, TimeUnit.MILLISECONDS));
        } catch (Exception cause) {
            fail("Should not throw an error");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "conservative", "balanced", "progressive" })
    public void testTimedGetWhenCompleteWithZeroTimeout(String futureType) {
        final ClientFutureFactory futuresFactory = ClientFutureFactory.create(futureType);
        final ClientFuture<Void> future = futuresFactory.createFuture();

        future.complete(null);

        try {
            assertNull(future.get(0, TimeUnit.MILLISECONDS));
        } catch (Exception cause) {
            fail("Should not throw an error");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "conservative", "balanced", "progressive" })
    public void testTimedGetWhenNotCompleteWithZeroTimeout(String futureType) {
        final ClientFutureFactory futuresFactory = ClientFutureFactory.create(futureType);
        final ClientFuture<Void> future = futuresFactory.createFuture();

        try {
            assertNull(future.get(0, TimeUnit.MILLISECONDS));
        } catch (Exception cause) {
            fail("Should not throw an error");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "conservative", "balanced", "progressive" })
    public void testOnFailure(String futureType) {
        final ClientFutureFactory futuresFactory = ClientFutureFactory.create(futureType);
        final ClientFuture<Void> future = futuresFactory.createFuture();
        final ClientException ex = new ClientException("Failed");

        future.failed(ex);
        try {
            future.get(5, TimeUnit.SECONDS);
            fail("Should throw an error");
        } catch (ExecutionException exe) {
            assertSame(exe.getCause(), ex);
        } catch (InterruptedException e) {
            fail("Should not throw an ExecutionException");
        } catch (TimeoutException e) {
            fail("Should not throw an ExecutionException");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "conservative", "balanced", "progressive" })
    public void testOnSuccessCallsSynchronization(String futureType) {
        final AtomicBoolean syncCalled = new AtomicBoolean(false);
        final ClientFutureFactory futuresFactory = ClientFutureFactory.create(futureType);

        final ClientFuture<Void> future = futuresFactory.createFuture(new ClientSynchronization() {

            @Override
            public void onPendingSuccess() {
                syncCalled.set(true);
            }

            @Override
            public void onPendingFailure(Throwable cause) {

            }
        });

        future.complete(null);
        try {
            future.get(5, TimeUnit.SECONDS);
        } catch (Exception cause) {
            fail("Should not throw an error");
        }

        assertTrue(syncCalled.get(), "Synchronization not called");
    }

    @ParameterizedTest
    @ValueSource(strings = { "conservative", "balanced", "progressive" })
    public void testOnFailureCallsSynchronization(String futureType) {
        final AtomicBoolean syncCalled = new AtomicBoolean(false);
        final ClientFutureFactory futuresFactory = ClientFutureFactory.create(futureType);

        final ClientFuture<Void> future = futuresFactory.createFuture(new ClientSynchronization() {

            @Override
            public void onPendingSuccess() {
            }

            @Override
            public void onPendingFailure(Throwable cause) {
                syncCalled.set(true);
            }
        });

        final ClientException ex = new ClientException("Failed");

        future.failed(ex);
        try {
            future.get(5, TimeUnit.SECONDS);
            fail("Should throw an error");
        } catch (ProviderException cause) {
        } catch (InterruptedException e) {
            fail("Should not throw an ExecutionException");
        } catch (ExecutionException e) {
            assertSame(e.getCause(), ex);
        } catch (TimeoutException e) {
            fail("Should not throw an ExecutionException");
        }

        assertTrue(syncCalled.get(), "Synchronization not called");
    }

    @ParameterizedTest
    @ValueSource(strings = { "conservative", "balanced", "progressive" })
    public void testSuccessfulStateIsFixed(String futureType) {
        final ClientFutureFactory futuresFactory = ClientFutureFactory.create(futureType);
        final ClientFuture<Void> future = futuresFactory.createFuture();
        final ClientException ex = new ClientException("Failed");

        future.complete(null);
        future.failed(ex);
        try {
            future.get(5, TimeUnit.SECONDS);
        } catch (Exception cause) {
            fail("Should not throw an error");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "conservative", "balanced", "progressive" })
    public void testFailedStateIsFixed(String futureType) {
        final ClientFutureFactory futuresFactory = ClientFutureFactory.create(futureType);
        final ClientFuture<Void> future = futuresFactory.createFuture();
        final ClientException ex = new ClientException("Failed");

        future.failed(ex);
        future.complete(null);
        try {
            future.get(5, TimeUnit.SECONDS);
            fail("Should throw an error");
        } catch (InterruptedException e) {
            fail("Should have thrown an execution exception");
        } catch (ExecutionException e) {
            assertSame(e.getCause(), ex);
        } catch (TimeoutException e) {
            fail("Should have thrown an execution exception");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "conservative", "balanced", "progressive" })
    public void testSyncHandlesInterruption(String futureType) throws InterruptedException {
        final ClientFutureFactory futuresFactory = ClientFutureFactory.create(futureType);
        final ClientFuture<Void> future = futuresFactory.createFuture();

        final CountDownLatch syncing = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean(false);

        Thread runner = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    syncing.countDown();
                    future.get();
                } catch (InterruptedException cause) {
                    Thread.interrupted();
                    interrupted.set(true);
                } catch (ExecutionException e) {
                } finally {
                    done.countDown();
                }
            }
        });

        runner.start();
        assertTrue(syncing.await(5, TimeUnit.SECONDS));
        runner.interrupt();

        assertTrue(done.await(5, TimeUnit.SECONDS));

        assertTrue(interrupted.get());
    }

    @ParameterizedTest
    @ValueSource(strings = { "conservative", "balanced", "progressive" })
    public void testTimedSyncHandlesInterruption(String futureType) throws InterruptedException {
        final ClientFutureFactory futuresFactory = ClientFutureFactory.create(futureType);
        final ClientFuture<Void> future = futuresFactory.createFuture();

        final CountDownLatch syncing = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean(false);

        Thread runner = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    syncing.countDown();
                    future.get(20, TimeUnit.SECONDS);
                } catch (InterruptedException cause) {
                    Thread.interrupted();
                    interrupted.set(true);
                } catch (ExecutionException e) {
                } catch (TimeoutException e) {
                } finally {
                    done.countDown();
                }
            }
        });

        runner.start();
        assertTrue(syncing.await(5, TimeUnit.SECONDS));
        runner.interrupt();

        assertTrue(done.await(5, TimeUnit.SECONDS));

        assertTrue(interrupted.get());
    }
}
