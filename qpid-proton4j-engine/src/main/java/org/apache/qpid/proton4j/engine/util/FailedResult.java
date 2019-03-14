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
package org.apache.qpid.proton4j.engine.util;

import org.apache.qpid.proton4j.engine.AsyncEvent;

/**
 * Utility AsyncResult used for failure results that are created in-place
 *
 * @param <E> The type of the result value for this {@link AsyncEvent}
 */
public class FailedResult<E> implements AsyncEvent<E> {

    private final Throwable cause;

    public FailedResult(Throwable cause) {
        this.cause = cause;
    }

    @Override
    public E get() {
        return null;
    }

    @Override
    public Throwable error() {
        return cause;
    }

    @Override
    public boolean succeeded() {
        return false;
    }

    @Override
    public boolean failed() {
        return true;
    }
}
