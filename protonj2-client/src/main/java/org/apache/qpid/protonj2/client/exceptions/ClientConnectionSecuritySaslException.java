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

/**
 * Security Exception used to indicate a security violation has occurred.
 */
public class ClientConnectionSecuritySaslException extends ClientConnectionSecurityException {

    private static final long serialVersionUID = 313318720407251822L;

    private boolean temporary;

    /**
     * Create a new instance of the connection SASL security exception
     *
     * @param message
     * 		The message that describes the error.
     */
    public ClientConnectionSecuritySaslException(String message) {
        this(message,false, null);
    }

    /**
     * Create a new instance of the connection SASL security exception
     *
     * @param message
     * 		The message that describes the error.
     * @param cause
     * 		The exception that initiated this error.
     */
    public ClientConnectionSecuritySaslException(String message, Throwable cause) {
        this(message,false, cause);
    }

    /**
     * Create a new instance of the connection SASL security exception
     *
     * @param message
     * 		The message that describes the error.
     * @param temporary
     * 		Boolean that indicates if the error is a temporary (true) or permanent error (false).
     */
    public ClientConnectionSecuritySaslException(String message, boolean temporary) {
        this(message, temporary, null);
    }

    /**
     * Create a new instance of the connection SASL security exception
     *
     * @param message
     * 		The message that describes the error.
     * @param temporary
     * 		Boolean that indicates if the error is a temporary (true) or permanent error (false).
     * @param cause
     * 		The exception that initiated this error.
     */
    public ClientConnectionSecuritySaslException(String message, boolean temporary, Throwable cause) {
        super(message, cause);

        this.temporary = temporary;
    }

    /**
     * @return true if the error is temporary and reconection may be possible.
     */
    public boolean isSysTempFailure() {
        return temporary;
    }
}
