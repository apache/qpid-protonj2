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

/**
 * Connection level Security Exception used to indicate a security violation has occurred.
 */
public class ClientConnectionSecurityException extends ClientConnectionRemotelyClosedException {

    private static final long serialVersionUID = -1895132556606592253L;

    /**
     * Creates a new connection security exception.
     *
     * @param message
     * 		The message that describes the reason for the security error.
     */
    public ClientConnectionSecurityException(String message) {
        super(message);
    }

    /**
     * Creates a new connection security exception.
     *
     * @param message
     * 		The message that describes the reason for the security error.
     * @param cause
     * 		An exception that further defines the reason for the security error.
     */
    public ClientConnectionSecurityException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new connection security exception.
     *
     * @param message
     * 		The message that describes the reason for the error.
     * @param errorCondition
     * 		An {@link ErrorCondition} that provides additional information about the error.
     */
    public ClientConnectionSecurityException(String message, ErrorCondition errorCondition) {
        super(message, errorCondition);
    }

    /**
     * Creates a new connection security exception.
     *
     * @param message
     * 		The message that describes the reason for the error.
     * @param cause
     * 		An exception that further defines the reason for the security error.
     * @param errorCondition
     * 		An {@link ErrorCondition} that provides additional information about the error.
     */
    public ClientConnectionSecurityException(String message, Throwable cause, ErrorCondition errorCondition) {
        super(message, cause, errorCondition);
    }
}
