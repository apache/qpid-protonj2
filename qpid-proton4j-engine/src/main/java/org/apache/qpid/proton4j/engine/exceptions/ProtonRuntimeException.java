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

/**
 * {@link RuntimeException} implementation for Proton to use when failing in calls
 * where a checked exception cannot be thrown.  This exception should always wrap are
 * {@link ProtonException} which originated this error.
 */
public class ProtonRuntimeException extends RuntimeException {

    private static final long serialVersionUID = -3330665302799464453L;

    /**
     * Creates a new ProtonRuntimeException with the given cause.
     *
     * @param cause
     *      The {@link ProtonException} that caused this error to be thrown.
     */
    public ProtonRuntimeException(ProtonException cause) {
        super(cause);
    }

    /**
     * Creates a new ProtonRuntimeException with the given cause.
     *
     * @param message
     *      A message that describes the exception condition.
     * @param cause
     *      The {@link ProtonException} that caused this error to be thrown.
     */
    public ProtonRuntimeException(String message, ProtonException cause) {
        super(message, cause);
    }

    @Override
    public ProtonException getCause() {
        return (ProtonException) super.getCause();
    }
}
