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
package org.apache.qpid.proton4j.engine.exceptions;

import javax.security.sasl.SaslException;

public class ProtonExceptionSupport {

    /**
     * Checks the given cause to determine if it's already an ProtonException type and
     * if not creates a new ProtonException to wrap it.
     *
     * @param cause
     *        The initiating exception that should be cast or wrapped.
     *
     * @return an ProtonException instance.
     */
    public static ProtonException createOrPassthrough(Throwable cause) {
        if (cause instanceof ProtonException) {
            return (ProtonException) cause;
        }

        if (cause.getCause() instanceof ProtonException) {
            return (ProtonException) cause.getCause();
        }

        String message = cause.getMessage();
        if (message == null || message.length() == 0) {
            message = cause.toString();
        }

        if (cause instanceof SaslException) {
            return new EngineSaslAuthenticationException(message, (SaslException) cause);
        } else {
            return new ProtonException(message, cause);
        }
    }

    /**
     * Checks the given cause to determine if it's already an ProtonException type and
     * if not creates a new ProtonException to wrap it.
     *
     * @param cause
     *        The initiating exception that should be cast or wrapped.
     *
     * @return an ProtonException instance.
     */
    public static EngineFailedException createFailedException(Throwable cause) {
        return createFailedException(null, cause);
    }

    /**
     * Checks the given cause to determine if it's already an EngineFailedException type and
     * if not creates a new EngineFailedException to wrap it.
     *
     * @param message
     *        A descriptive message to be applied to the returned exception.
     * @param cause
     *        The initiating exception that should be cast or wrapped.
     *
     * @return an ProtonException instance.
     */
    public static EngineFailedException createFailedException(String message, Throwable cause) {
        if (cause instanceof EngineFailedException) {
            return (EngineFailedException) cause;
        }

        if (cause.getCause() instanceof EngineFailedException) {
            return (EngineFailedException) cause.getCause();
        }

        if (message == null || message.isEmpty()) {
            message = cause.getMessage();
            if (message == null || message.length() == 0) {
                message = cause.toString();
            }
        }

        if (cause instanceof SaslException) {
            return new EngineSaslAuthenticationException(message, (SaslException) cause);
        } else {
            return new EngineFailedException(message, cause);
        }
    }
}
