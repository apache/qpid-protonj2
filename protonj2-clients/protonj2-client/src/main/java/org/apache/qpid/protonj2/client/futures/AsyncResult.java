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

import org.apache.qpid.protonj2.client.exceptions.ClientException;

/**
 * Defines a result interface for Asynchronous operations.
 *
 * @param <V> Type used to complete the future.
 */
public interface AsyncResult<V> {

    /**
     * If the operation fails this method is invoked with the Exception
     * that caused the failure.
     *
     * @param result
     *        The error that resulted in this asynchronous operation failing.
     */
    void failed(ClientException result);

    /**
     * If the operation succeeds the resulting value produced is set to null and
     * the waiting parties are signaled.
     *
     * @param result
     *      the object that completes the future.
     */
    void complete(V result);

    /**
     * Returns true if the AsyncResult has completed.  The task is considered complete
     * regardless if it succeeded or failed.
     *
     * @return returns true if the asynchronous operation has completed.
     */
    boolean isComplete();

}
