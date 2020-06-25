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

public interface AmqpError {

    /**
     * An internal error occurred. Operator intervention might be necessary to resume normal operation.
     */
    Symbol INTERNAL_ERROR = Symbol.valueOf("amqp:internal-error");

    /**
     * A peer attempted to work with a remote entity that does not exist.
     */
    Symbol NOT_FOUND = Symbol.valueOf("amqp:not-found");

    /**
     * A peer attempted to work with a remote entity to which it has no access due to security settings.
     */
    Symbol UNAUTHORIZED_ACCESS = Symbol.valueOf("amqp:unauthorized-access");

    /**
     * Data could not be decoded.
     */
    Symbol DECODE_ERROR = Symbol.valueOf("amqp:decode-error");

    /**
     * A peer exceeded its resource allocation.
     */
    Symbol RESOURCE_LIMIT_EXCEEDED = Symbol.valueOf("amqp:resource-limit-exceeded");

    /**
     * The peer tried to use a frame in a manner that is inconsistent with the semantics defined in the specification.
     */
    Symbol NOT_ALLOWED = Symbol.valueOf("amqp:not-allowed");

    /**
     * An invalid field was passed in a frame body, and the operation could not proceed.
     */
    Symbol INVALID_FIELD = Symbol.valueOf("amqp:invalid-field");

    /**
     * The peer tried to use functionality that is not implemented in its partner.
     */
    Symbol NOT_IMPLEMENTED = Symbol.valueOf("amqp:not-implemented");

    /**
     * The client attempted to work with a server entity to which it has no access because another client is working with it.
     */
    Symbol RESOURCE_LOCKED = Symbol.valueOf("amqp:resource-locked");

    /**
     * The client made a request that was not allowed because some precondition failed.
     */
    Symbol PRECONDITION_FAILED = Symbol.valueOf("amqp:precondition-failed");

    /**
     * A server entity the client is working with has been deleted.
     */
    Symbol RESOURCE_DELETED = Symbol.valueOf("amqp:resource-deleted");

    /**
     * The peer sent a frame that is not permitted in the current state.
     */
    Symbol ILLEGAL_STATE = Symbol.valueOf("amqp:illegal-state");

    /**
     * The peer cannot send a frame because the smallest encoding of the performative with the currently valid
     * values would be too large to fit within a frame of the agreed maximum frame size. When transferring a message
     * the message data can be sent in multiple transfer frames thereby avoiding this error. Similarly when attaching
     * a link with a large unsettled map the endpoint MAY make use of the incomplete-unsettled flag to avoid the need
     * for overly large frames.
     */
    Symbol FRAME_SIZE_TOO_SMALL = Symbol.valueOf("amqp:frame-size-too-small");

}
