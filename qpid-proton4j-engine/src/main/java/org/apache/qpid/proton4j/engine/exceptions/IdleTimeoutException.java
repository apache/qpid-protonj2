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
 * Error thrown when the Engine idle checking detects a timeout condition and
 * shuts down the engine and places it in an error state.
 */
public class IdleTimeoutException extends ProtonException {

    private static final long serialVersionUID = 6527918786644498627L;

    public IdleTimeoutException() {
    }

    public IdleTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public IdleTimeoutException(String message) {
        super(message);
    }

    public IdleTimeoutException(Throwable cause) {
        super(cause);
    }
}
