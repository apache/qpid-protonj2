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
package org.apache.qpid.protonj2.types.transport;

import org.apache.qpid.protonj2.types.Symbol;

public interface SessionError {

    /**
     * The peer violated incoming window for the session.
     */
    Symbol WINDOW_VIOLATION = Symbol.valueOf("amqp:session:window-violation");

    /**
     * Input was received for a link that was detached with an error.
     */
    Symbol ERRANT_LINK = Symbol.valueOf("amqp:session:errant-link");

    /**
     * An attach was received using a handle that is already in use for an attached link.
     */
    Symbol HANDLE_IN_USE = Symbol.valueOf("amqp:session:handle-in-use");

    /**
     * A frame (other than attach) was received referencing a handle which is not currently in use of an attached link.
     */
    Symbol UNATTACHED_HANDLE = Symbol.valueOf("amqp:session:unattached-handle");

}
