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

public interface ConnectionError {

    /**
     * An operator intervened to close the connection for some reason. The client could retry at some later date.
     */
    Symbol CONNECTION_FORCED = Symbol.valueOf("amqp:connection:forced");

    /**
     * A valid frame header cannot be formed from the incoming byte stream.
     */
    Symbol FRAMING_ERROR = Symbol.valueOf("amqp:connection:framing-error");

    /**
     * The container is no longer available on the current connection. The peer SHOULD
     * attempt reconnection to the container using the details provided in the info map.
     * <br>
     * <ul>
     *   <li>hostname:
     *     <ul>
     *       <li>the hostname of the container hosting the terminus. This is the value that SHOULD be
     *           supplied in the hostname field of the open frame, and during SASL and TLS negotiation
     *           (if used).
     *     </ul>
     *   <li>network-host:
     *     <ul>
     *       <li>the DNS hostname or IP address of the machine hosting the container.
     *     </ul>
     *   <li>port:
     *     <ul>
     *       <li>the port number on the machine hosting the container.
     *     </ul>
     * </ul>
     */
    Symbol REDIRECT = Symbol.valueOf("amqp:connection:redirect");

}
