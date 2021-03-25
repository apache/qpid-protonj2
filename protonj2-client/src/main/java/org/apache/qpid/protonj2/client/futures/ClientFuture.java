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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.qpid.protonj2.client.exceptions.ClientException;

/**
 * Asynchronous Client Future class.
 *
 * @param <V> the eventual result type for this Future
 */
public abstract class ClientFuture<V> implements Future<V>, AsyncResult<V> {

    private static final ClientException UNSPECIFIED_ERROR = new ClientException("Failed with an unspecified error");

    protected final ClientSynchronization<V> synchronization;

    // States used to track progress of this future
    protected static final int INCOMPLETE = 0;
    protected static final int COMPLETING = 1;
    protected static final int SUCCESS = 2;
    protected static final int FAILURE = 3;
    protected static final int CANCELLED = 4;

    @SuppressWarnings("rawtypes")
    protected static final AtomicIntegerFieldUpdater<ClientFuture> STATE_FIELD_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(ClientFuture.class,"state");

    private volatile int state = INCOMPLETE;
    protected ExecutionException error;
    protected int waiting;
    protected V result;

    public ClientFuture() {
        this(null);
    }

    public ClientFuture(ClientSynchronization<V> synchronization) {
        this.synchronization = synchronization;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (STATE_FIELD_UPDATER.compareAndSet(this, INCOMPLETE, COMPLETING)) {
            STATE_FIELD_UPDATER.lazySet(this, CANCELLED);

            synchronized(this) {
                if (waiting > 0) {
                    notifyAll();
                }
            }

            return true;
        } else {
            return false;
        }
    }

    public boolean isFailed() {
        return error != null;
    }

    public V getResult() {
        return result;
    }

    @Override
    public boolean isCancelled() {
        return state > FAILURE;
    }

    @Override
    public boolean isDone() {
        return isComplete() || isCancelled() || isFailed();
    }

    @Override
    public boolean isComplete() {
        return state > COMPLETING;
    }

    /**
     * @return the current {@link ClientFuture} state as if this call.
     */
    protected int getState() {
        return state;
    }

    @Override
    public void failed(ClientException result) {
        if (STATE_FIELD_UPDATER.compareAndSet(this, INCOMPLETE, COMPLETING)) {
            error = new ExecutionException(result != null ? result : UNSPECIFIED_ERROR);

            if (synchronization != null) {
                synchronization.onPendingFailure(error);
            }

            STATE_FIELD_UPDATER.lazySet(this, FAILURE);

            synchronized(this) {
                if (waiting > 0) {
                    notifyAll();
                }
            }
        }
    }

    @Override
    public void complete(V result) {
        if (STATE_FIELD_UPDATER.compareAndSet(this, INCOMPLETE, COMPLETING)) {
            this.result = result;

            if (synchronization != null) {
                synchronization.onPendingSuccess(result);
            }

            STATE_FIELD_UPDATER.lazySet(this, SUCCESS);

            synchronized(this) {
                if (waiting > 0) {
                    notifyAll();
                }
            }
        }
    }

    @Override
    public abstract V get() throws InterruptedException, ExecutionException;

    @Override
    public abstract V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;

    /**
     * TODO - Provide hook to run on the event loop to do whatever it means to cancel this task and
     *        update the task state in a thread safe manner.
     */
    protected void tryCancelTask() {

    }
}
