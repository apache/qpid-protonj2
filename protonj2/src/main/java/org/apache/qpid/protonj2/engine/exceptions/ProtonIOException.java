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

/**
 * Thrown when an unchecked IO exception is encountered from a write event that
 * invokes an outside write handler from the engine.
 */
public class ProtonIOException extends ProtonException {

    private static final long serialVersionUID = -8458098856099248658L;

    public ProtonIOException() {
        super();
    }

    public ProtonIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProtonIOException(String message) {
        super(message);
    }

    public ProtonIOException(Throwable cause) {
        super(cause);
    }
}
