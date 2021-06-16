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
 * Thrown when a client attempt to commit or roll-back when no transaction has been declared.
 */
public class ClientTransactionNotActiveException extends ClientIllegalStateException {

    private static final long serialVersionUID = 7854401747821768051L;

    /**
     * Creates a new transaction not active exception.
     *
     * @param message
     * 		The message that describes the reason for the error.
     */
    public ClientTransactionNotActiveException(String message) {
        super(message);
    }

    /**
     * Creates a new transaction not active exception.
     *
     * @param message
     * 		The message that describes the reason for the error.
     * @param cause
     * 		An exception that further defines the reason for the error.
     */
    public ClientTransactionNotActiveException(String message, Throwable cause) {
        super(message, cause);
    }
}
