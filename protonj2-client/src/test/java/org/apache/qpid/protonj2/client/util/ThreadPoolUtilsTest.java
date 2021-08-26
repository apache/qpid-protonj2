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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test for ThreadPoolUtils support class.
 */
@Timeout(10)
public class ThreadPoolUtilsTest extends ImperativeClientTestCase {

    @Test
    public void testCreate() {
        new ThreadPoolUtils();
    }

    @Test
    public void testShutdown() throws Exception {
        ExecutorService service = Executors.newSingleThreadExecutor();
        ThreadPoolUtils.shutdown(service);
        assertTrue(service.isShutdown());
    }

    @Test
    public void testShutdownNullService() throws Exception {
        ThreadPoolUtils.shutdown(null);
    }

    @Test
    public void testShutdownNowWithNoTasks() throws Exception {
        ExecutorService service = Executors.newSingleThreadExecutor();
        assertNotNull(ThreadPoolUtils.shutdownNow(service));
        assertTrue(service.isShutdown());
    }

    @Test
    public void testShutdownNowReturnsUnexecuted() throws Exception {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch finish = new CountDownLatch(1);
        ExecutorService service = Executors.newSingleThreadExecutor();

        service.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    started.countDown();
                    finish.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                }
            }
        });

        service.execute(new Runnable() {

            @Override
            public void run() {
            }
        });
        service.execute(new Runnable() {

            @Override
            public void run() {
            }
        });

        assertTrue(started.await(5, TimeUnit.SECONDS));

        List<Runnable> notRun = ThreadPoolUtils.shutdownNow(service);
        assertTrue(service.isShutdown());
        finish.countDown();
        assertTrue(ThreadPoolUtils.awaitTermination(service, 1000));

        assertEquals(2, notRun.size());
    }

    @Test
    public void testShutdownNowAlreadyShutdown() throws Exception {
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.shutdown();
        assertNotNull(ThreadPoolUtils.shutdownNow(service));
        assertTrue(service.isShutdown());
    }

    @Test
    public void testShutdownNowNullService() throws Exception {
        assertNotNull(ThreadPoolUtils.shutdownNow(null));
    }

    @Test
    public void testShutdownGraceful() throws Exception {
        ExecutorService service = Executors.newSingleThreadExecutor();
        ThreadPoolUtils.shutdownGraceful(service);
        assertTrue(service.isShutdown());
    }

    @Test
    public void testShutdownGracefulWithTimeout() throws Exception {
        ExecutorService service = Executors.newSingleThreadExecutor();
        ThreadPoolUtils.shutdownGraceful(service, 1000);
        assertTrue(service.isShutdown());
    }

    @Test
    public void testShutdownGracefulWithStuckTask() throws Exception {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch finish = new CountDownLatch(1);
        ExecutorService service = Executors.newSingleThreadExecutor();

        service.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    started.countDown();
                    finish.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                }
            }
        });

        assertTrue(started.await(5, TimeUnit.SECONDS));

        ThreadPoolUtils.shutdownGraceful(service, 100);
        assertTrue(service.isShutdown());
        finish.countDown();
        assertTrue(ThreadPoolUtils.awaitTermination(service, 1000));
    }

    @Test
    public void testAwaitTerminationWithNullService() throws Exception {
        assertTrue(ThreadPoolUtils.awaitTermination(null, 1000));
    }
}
