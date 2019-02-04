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
package org.apache.qpid.proton4j.engine.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.engine.AsyncResult;
import org.apache.qpid.proton4j.engine.Endpoint;
import org.apache.qpid.proton4j.engine.EndpointState;
import org.apache.qpid.proton4j.engine.EventHandler;

/**
 * Basic functionality for each of the end point objects.
 */
public abstract class ProtonEndpoint<T extends Endpoint<T>> implements Endpoint<T> {

    private Object context;
    private Map<String, Object> contextEntries = new HashMap<>();

    private EndpointState localState = EndpointState.IDLE;
    private EndpointState remoteState = EndpointState.IDLE;

    private ErrorCondition localError = new ErrorCondition();
    private ErrorCondition remoteError = new ErrorCondition();

    @Override
    public void setContext(Object context) {
        this.context = context;
    }

    @Override
    public Object getContext() {
        return context;
    }

    @Override
    public void setContextEntry(String key, Object value) {
        contextEntries.put(key, value);
    }

    @Override
    public Object getContextEntry(String key) {
        return contextEntries.get(key);
    }

    @Override
    public EndpointState getLocalState() {
        return localState;
    }

    @Override
    public ErrorCondition getLocalCondition() {
        return localError;
    }

    void setLocalCondition(ErrorCondition condition) {
        if (condition != null) {
            localError = condition.copy();
        } else {
            localError.clear();
        }
    }

    @Override
    public EndpointState getRemoteState() {
        return remoteState;
    }

    @Override
    public ErrorCondition getRemoteCondition() {
        return remoteError;
    }

    void setRemoteCondition(ErrorCondition condition) {
        if (condition != null) {
            remoteError = condition.copy();
        } else {
            remoteError.clear();
        }
    }

    @Override
    public void open(EventHandler<AsyncResult<T>> handler) {
        // TODO - This is to vague of a state it could already be closed
        if (getLocalState() != EndpointState.ACTIVE) {
            localState = EndpointState.ACTIVE;
            initiateLocalOpen();
        }
    }

    @Override
    public void close() {
        // TODO - This is to vague of a state it might have never been opened.
        if (getLocalState() != EndpointState.CLOSED) {
            localState = EndpointState.CLOSED;
            initiateLocalClose();
        }
    }

    abstract void initiateLocalOpen();

    abstract void initiateLocalClose();

}
