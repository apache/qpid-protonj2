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
package org.apache.qpid.protonj2.client;

/**
 * A {@link Message} type that supports writing the message contents in multiple
 * chunks as the message data becomes available.
 *
 * @param <E>
 */
public interface StreamableMessage<E> extends AdvancedMessage<E> {

    /**
     * Marks the currently streaming message as being aborted.
     * <p>
     * Simply marking a {@link StreamableMessage} as being aborted does not signal
     * the remote peer that the message was aborted, the message must be sent a final
     * time using the {@link Sender} that was used to stream it originally.  A
     * {@link StreamableMessage} cannot be aborted following the final write of the
     * message payload.
     *
     * @return this {@link StreamableMessage} instance.
     */
    @Override
    StreamableMessage<E> abort();

    /**
     * @return true if this message has been marked as aborted previously.
     */
    @Override
    boolean aborted();

    /**
     * Marks the currently streaming message as being complete.
     * <p>
     * Simply marking a {@link StreamableMessage} as being complete does not signal
     * the remote peer that the message was completed, the message must be sent a final
     * time using the {@link Sender} that was used to stream it originally.  A
     * {@link StreamableMessage} cannot be completed following a message write that
     * was marked as being aborted using the {@link #abort()} method.
     *
     * @return this {@link StreamableMessage} instance.
     */
    //StreamableMessage<E> complete();

    /**
     * @return true if this message has been marked as completed previously.
     */
    @Override
    boolean complete();

}
