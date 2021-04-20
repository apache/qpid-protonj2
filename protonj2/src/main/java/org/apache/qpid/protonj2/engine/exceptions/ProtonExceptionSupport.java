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
package org.apache.qpid.protonj2.engine.exceptions;

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
    public static EngineFailedException createFailedException(Throwable cause) {
        return createFailedException(null, cause);
    }

    /**
     * Creates a new instance of an EngineFailedException that either wraps the given cause
     * if it is not an instance of an {@link EngineFailedException} or creates a new copy
     * of the given {@link EngineFailedException} in order to produce a meaningful stack trace
     * for the caller of which of their calls failed due to the engine state being failed.
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
            return ((EngineFailedException) cause).duplicate();
        }

        if (cause.getCause() instanceof EngineFailedException) {
            return ((EngineFailedException) cause.getCause()).duplicate();
        }

        if (message == null || message.isEmpty()) {
            message = cause.getMessage();
            if (message == null || message.length() == 0) {
                message = cause.toString();
            }
        }

        return new EngineFailedException(message, cause);
    }
}
