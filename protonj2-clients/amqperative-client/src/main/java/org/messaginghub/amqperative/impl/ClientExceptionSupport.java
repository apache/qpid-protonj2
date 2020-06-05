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
package org.messaginghub.amqperative.impl;

import org.messaginghub.amqperative.exceptions.ClientIOException;

public class ClientExceptionSupport {

    /**
     * Checks the given cause to determine if it's already an ProviderIOException type and
     * if not creates a new ProviderIOException to wrap it.
     *
     * @param cause
     *        The initiating exception that should be cast or wrapped.
     *
     * @return an ProviderIOException instance.
     */
    public static ClientIOException createOrPassthroughFatal(Throwable cause) {
        if (cause instanceof ClientIOException) {
            return (ClientIOException) cause;
        }

        if (cause.getCause() instanceof ClientIOException) {
            return (ClientIOException) cause.getCause();
        }

        String message = cause.getMessage();
        if (message == null || message.length() == 0) {
            message = cause.toString();
        }

        return new ClientIOException(message, cause);
    }

    /**
     * Checks the given cause to determine if it's already an ProviderException type and
     * if not creates a new ProviderException to wrap it.  If the inbound exception is a
     * fatal type then it will pass through this method untouched to preserve the fatal
     * status of the error.
     *
     * @param cause
     *        The initiating exception that should be cast or wrapped.
     *
     * @return an ProviderException instance.
     */
    public static ClientException createNonFatalOrPassthrough(Throwable cause) {
        if (cause instanceof ClientException) {
            return (ClientException) cause;
        }

        if (cause.getCause() instanceof ClientException) {
            return (ClientException) cause.getCause();
        }

        String message = cause.getMessage();
        if (message == null || message.length() == 0) {
            message = cause.toString();
        }

        return new ClientException(message, cause);
    }
}
