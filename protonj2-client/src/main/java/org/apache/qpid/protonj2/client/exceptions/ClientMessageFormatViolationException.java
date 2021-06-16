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

import org.apache.qpid.protonj2.client.Message;

/**
 * Exception thrown from {@link Message} instances when the body section specified
 * violates the configure message format of the message that is being created.
 */
public class ClientMessageFormatViolationException extends ClientException {

    private static final long serialVersionUID = -7731216779946825581L;

    /**
     * Creates a new connection message format violation exception.
     *
     * @param message
     * 		The message that describes the reason for the error.
     */
    public ClientMessageFormatViolationException(String message) {
        super(message);
    }

    /**
     * Creates a new connection message format violation exception.
     *
     * @param message
     * 		The message that describes the reason for the error.
     * @param cause
     * 		An exception that further defines the reason for the error.
     */
    public ClientMessageFormatViolationException(String message, Throwable cause) {
        super(message, cause);
    }
}
