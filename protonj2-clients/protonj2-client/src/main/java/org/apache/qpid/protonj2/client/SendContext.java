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

import java.io.OutputStream;

import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * A send context allows for control over a {@link Message} send where the contents to
 * be sent can span multiple {@link Transfer} frames that are sent to the remote peer.
 * In order to perform a multiple framed {@link Message} send a new {@link SendContext}
 * must be created at the start of each new message boundary.
 */
public interface SendContext {

    /**
     * Following the first send of an {@link AdvancedMessage} for this {@link SendContext}
     * the context will have an assigned {@link Tracker} that can be used to monitor the
     * remote state of the delivery that comprises the multiple framed message transfer.
     *
     * @return the {@link Tracker} instance assigned to this {@link SendContext}
     */
    Tracker tracker();

    /**
     * Encodes and sends the contents of the given message as a portion of the total
     * payload of this {@link SendContext}.
     *
     * @param message
     *      The {@link AdvancedMessage} whose contents should be sent.
     *
     * @return this {@link SendContext} instance.
     *
     * @throws ClientException if an error occurs while initiating the send operation.
     */
    <E> SendContext send(AdvancedMessage<E> message) throws ClientException;

    /**
     * Marks the currently streaming message as being aborted.
     * <p>
     * Simply marking a {@link AdvancedMessage} as being aborted does not signal
     * the remote peer that the message was aborted, the message must be sent a final
     * time using the {@link Sender} that was used to stream it originally.  A
     * {@link AdvancedMessage} cannot be aborted following a send where the complete
     * flag was set to true (default value).
     *
     * @param aborted
     *      Should the message be marked as having been aborted.
     *
     * @return this {@link AdvancedMessage} instance.
     *
     * @throws ClientException if an error occurs while initiating the abort operation.
     */
    Tracker abort() throws ClientException;

    /**
     * @return true if this {@link SendContext} has been marked as aborted previously.
     */
    boolean aborted();

    /**
     * Marks the currently streaming message as being complete.
     * <p>
     * Marking a message as complete finalizes the {@link SendContext} and causes a
     * final {@link Transfer} frame to be sent to the remote indicating that the ongoing
     * streaming delivery is done and no more message data will arrive.
     *
     * @return the Tracker that is associated with this {@link SendContext} instance.
     *
     * @throws ClientException if an error occurs while initiating the completion operation.
     */
    Tracker complete() throws ClientException;

    /**
     * Marks the currently streaming message as being complete the encoded form of the
     * provided message will be sent along with the final {@link Transfer} marked as being
     * completed.
     * <p>
     * Marking a message as complete finalizes the {@link SendContext} and causes a
     * final {@link Transfer} frame to be sent to the remote indicating that the ongoing
     * streaming delivery is done and no more message data will arrive.
     *
     * @param message
     *      The final Message payload to transit along with the complete indicator.
     *
     * @return the Tracker that is associated with this {@link SendContext} instance.
     *
     * @throws ClientException if an error occurs while initiating the completion operation.
     */
    <E> Tracker complete(AdvancedMessage<E> message) throws ClientException;

    /**
     * @return true if this message has been marked as being complete.
     */
    boolean completed();

    /**
     * Creates an {@link MessageOutputStream} instance configured with the given options.
     * <p>
     * The {@link MessageOutputStream} can be used to write the payload of an AMQP Message in
     * chunks when the source is not readily available in memory or as part of a larger streams
     * based component.  The {@link Message} based stream allows for control over the AMQP
     * message {@link Section} values that are sent but does the encoding itself.  For stream
     * of message data where the content source already consists of an AMQP encoded message the
     * {@link RawOutputStream} should be used instead.
     *
     * @param options
     *      The stream options to use to configure the returned {@link MessageOutputStream}
     *
     * @return a {@link MessageOutputStream} instance configured using the given options.
     */
    MessageOutputStream outputStream(MessageOutputStreamOptions options);

    /**
     * Creates an {@link RawOutputStream} instance configured with the given options.
     * <p>
     * The {@link RawOutputStream} can be used to write the payload of an AMQP Message in
     * chunks when the source is not readily available in memory or as part of a larger streams
     * based component.  The source of the bytes written to the {@link RawOutputStream} should
     * consist of already encoded AMQP {@link Message} data.  For an {@link OutputStream} that
     * performs the encoding of message data refer to the {@link MessageOutputStream}.
     *
     * @param options
     *      The stream options to use to configure the returned {@link MessageOutputStream}
     *
     * @return a {@link RawOutputStream} instance configured using the given options.
     */
    RawOutputStream outputStream(RawOutputStreamOptions options);

}
