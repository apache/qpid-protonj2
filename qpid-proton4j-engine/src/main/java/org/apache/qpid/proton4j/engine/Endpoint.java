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
package org.apache.qpid.proton4j.engine;

import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;

/**
 * Represents one end of an AMQP Resource.
 */
public interface Endpoint {

    /**
     * Open the end point.
     */
    public void open();

    /**
     * Close the end point
     */
    public void close();

    /**
     * Sets an application defined context value that will be carried with this {@link Connection} until
     * cleared by the application.
     *
     * @param context
     *      The context to associate with this connection.
     */
    void setContext(Object context);

    /**
     * @return the currently configured context that is associated with this {@link Connection}
     */
    Object getContext();

    /**
     * Sets or updates a named application defined context value that will be carried with this
     * {@link Connection} until cleared by the application.
     *
     * @param key
     *      The key used to identify the given context entry.
     * @param value
     *      The context value to assigned to the given key, or null to clear.
     */
    void setContextEntry(String key, Object value);

    /**
     * @return the context entry assigned to the given key or null of none assigned.
     */
    Object getContextEntry(String key);

    /**
     * @return the local endpoint state
     */
    public EndpointState getLocalState();

    /**
     * @return the local endpoint error, or null if there is none
     */
    public ErrorCondition getLocalCondition();

    /**
     * @return the remote endpoint state (as last communicated)
     */
    public EndpointState getRemoteState();

    /**
     * @return the remote endpoint error, or null if there is none
     */
    public ErrorCondition getRemoteCondition();

}
