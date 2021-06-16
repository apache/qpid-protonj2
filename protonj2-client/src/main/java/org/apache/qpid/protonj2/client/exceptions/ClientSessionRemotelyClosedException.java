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
package org.apache.qpid.protonj2.client.exceptions;

import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.Session;

/**
 * Root exception type for cases of remote closure or client created {@link Session}.
 */
public class ClientSessionRemotelyClosedException extends ClientResourceRemotelyClosedException {

    private static final long serialVersionUID = 5601827103553513599L;

    /**
     * Creates a new session remotely closed exception.
     *
     * @param message
     * 		The message that describes the reason for the remote closure.
     */
    public ClientSessionRemotelyClosedException(String message) {
        this(message, (ErrorCondition) null);
    }

    /**
     * Creates a new session remotely closed exception.
     *
     * @param message
     * 		The message that describes the reason for the remote closure.
     * @param cause
     * 		An exception that further defines the remote close reason.
     */
    public ClientSessionRemotelyClosedException(String message, Throwable cause) {
        this(message, cause, null);
    }

    /**
     * Creates a new session remotely closed exception.
     *
     * @param message
     * 		The message that describes the reason for the remote closure.
     * @param condition
     * 		An {@link ErrorCondition} that provides additional information about the close reason.
     */
    public ClientSessionRemotelyClosedException(String message, ErrorCondition condition) {
        super(message, condition);
    }

    /**
     * Creates a new session remotely closed exception.
     *
     * @param message
     * 		The message that describes the reason for the remote closure.
     * @param cause
     * 		An exception that further defines the remote close reason.
     * @param condition
     * 		An {@link ErrorCondition} that provides additional information about the close reason.
     */
    public ClientSessionRemotelyClosedException(String message, Throwable cause, ErrorCondition condition) {
        super(message, cause, condition);
    }
}
