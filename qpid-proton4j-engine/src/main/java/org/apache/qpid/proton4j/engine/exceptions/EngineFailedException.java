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
 * Thrown from Engine API methods that attempted an operation what would have
 * resulted in a write of data or other state modification after the engine has
 * entered the the failed state.
 */
public class EngineFailedException extends EngineStateException {

    private static final long serialVersionUID = 5947522999263302647L;

    public EngineFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public EngineFailedException(Throwable cause) {
        super(cause);
    }
}
