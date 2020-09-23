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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.apache.qpid.protonj2.types.transport.Disposition;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Streaming Message Tracker object used to operate on and track the state of a streamed message
 * at the remote. The {@link StreamSenderMessage} allows for local settlement and disposition management
 * as well as waiting for remote settlement of a streamed message.
 */
public interface StreamSenderMessage {

    /**
     * @return the {@link Sender} that was used to send the delivery that is being tracked.
     */
    StreamSender sender();

    /**
     * Returns the configured message format value that will be set on the first outgoing
     * AMQP {@link Transfer} frame for the delivery that comprises this streamed message.
     *
     * @return the configured message format that will be sent.
     */
    int messageFormat();

    /**
     * Sets the configured message format value that will be set on the first outgoing
     * AMQP {@link Transfer} frame for the delivery that comprises this streamed message.
     * This value can only be updated before write operation is attempted and will throw
     * an {@link ClientIllegalStateException} if any attempt to alter the value is made
     * following a write.
     *
     * @param messageFormat
     *      The assigned AMQP message format for this streamed message.
     *
     * @return this {@link StreamSenderMessage} instance.
     *
     * @throws ClientException if an error occurs while attempting set the message format.
     */
    StreamSenderMessage messageFormat(int messageFormat) throws ClientException;

    /**
     * Writes the given {@link Section} into the {@link SendContext}.  The the data written we
     * be sent to the remote peer when this {@link SendContext} is flushed or the configured send
     * buffer limit is reached.
     *
     * @param section
     *      The {@link Section} instance to be written into this send context..
     *
     * @return this {@link StreamSenderMessage} instance.
     *
     * @throws ClientException if an error occurs while attempting to write the section.
     */
    StreamSenderMessage write(Section<?> section) throws ClientException;

    /**
     * Sends all currently buffered message body data over the parent {@link Sender} link.
     *
     * @return this {@link StreamSenderMessage} instance.
     *
     * @throws ClientException if an error occurs while attempting to write the buffered contents.
     */
    StreamSenderMessage flush() throws ClientException;

    /**
     * Marks the currently streaming message as being complete.
     * <p>
     * Marking a message as complete finalizes the {@link SendContext} and causes a
     * final {@link Transfer} frame to be sent to the remote indicating that the ongoing
     * streaming delivery is done and no more message data will arrive.
     *
     * @return this {@link StreamSenderMessage} instance.
     *
     * @throws ClientException if an error occurs while initiating the completion operation.
     */
    StreamSenderMessage complete() throws ClientException;

    /**
     * @return true if this message has been marked as being complete.
     */
    boolean completed();

    /**
     * Marks the currently streaming message as being aborted. Once aborted no further
     * writes regardless of whether any writes have yet been performed or not.
     *
     * @param aborted
     *      Should the message be marked as having been aborted.
     *
     * @return this {@link StreamSenderMessage} instance.
     *
     * @throws ClientException if an error occurs while initiating the abort operation.
     */
    StreamSenderMessage abort() throws ClientException;

    /**
     * @return true if this {@link SendContext} has been marked as aborted previously.
     */
    boolean aborted();

    /**
     * Creates an {@link OutputStream} instance configured with the given options which will
     * write the bytes as the payload of one or more AMQP {@link Data} sections based on the
     * provided configuration..
     * <p>
     * The returned {@link OutputStream} can be used to write the payload of an AMQP Message in
     * chunks when the source is not readily available in memory or as part of a larger streams
     * based component.  The {@link Data} section based stream allows for control over the AMQP
     * message {@link Section} values that are sent but does the encoding itself.  For stream
     * of message data where the content source already consists of an AMQP encoded message refer
     * to the {@link #rawOutputStream(OutputStreamOptions)} method.
     *
     * @param options
     *      The stream options to use to configure the returned {@link MessageOutputStream}
     *
     * @return a {@link OutputStream} instance configured using the given options.
     *
     * @throws ClientException if an error occurs while creating the {@link OutputStream}.
     *
     * @see #rawOutputStream(OutputStreamOptions)
     */
    OutputStream dataOutputStream(OutputStreamOptions options) throws ClientException;

    /**
     * Creates an {@link OutputStream} instance configured with the given options that writes
     * the bytes given without additional encoding or transformation.
     * <p>
     * The returned {@link OutputStream} can be used to write the payload of an AMQP Message in
     * chunks when the source is not readily available in memory or as part of a larger streams
     * based component.  The source of the bytes written to the {@link OutputStream} should
     * consist of already encoded AMQP {@link Message} data.  For an {@link OutputStream} that
     * performs the encoding of message data refer to the {@link #dataOutputStream(OutputStreamOptions)}
     * method.
     *
     * @param options
     *      The stream options to use to configure the returned {@link MessageOutputStream}
     *
     * @return a {@link OutputStream} instance configured using the given options.
     *
     * @throws ClientException if an error occurs while creating the {@link OutputStream}.
     *
     * @see #dataOutputStream(OutputStreamOptions)
     */
    OutputStream rawOutputStream(OutputStreamOptions options) throws ClientException;

    /**
     * Settles the delivery locally, if not {@link SenderOptions#autoSettle() auto-settling}.
     *
     * @return this {@link StreamSenderMessage} instance.
     *
     * @throws ClientException if an error occurs while performing the settlement.
     */
    StreamSenderMessage settle() throws ClientException;

    /**
     * @return true if the sent message has been locally settled.
     */
    boolean settled();

    /**
     * Gets the current local state for the tracked delivery.
     *
     * @return the delivery state
     */
    DeliveryState state();

    /**
     * Gets the current remote state for the tracked delivery.
     *
     * @return the remote {@link DeliveryState} once a value is received from the remote.
     */
    DeliveryState remoteState();

    /**
     * Gets whether the delivery was settled by the remote peer yet.
     *
     * @return whether the delivery is remotely settled
     */
    boolean remoteSettled();

    /**
     * Updates the DeliveryState, and optionally settle the delivery as well.
     *
     * @param state
     *            the delivery state to apply
     * @param settle
     *            whether to {@link #settle()} the delivery at the same time
     *
     * @return this {@link StreamSenderMessage} instance.
     *
     * @throws ClientException
     */
    StreamSenderMessage disposition(DeliveryState state, boolean settle) throws ClientException;

    /**
     * Returns a future that can be used to wait for the remote to acknowledge receipt of
     * a sent message by settling it.
     *
     * @return a {@link Future} that can be used to wait on remote settlement.
     */
    Future<StreamSenderMessage> settlementFuture();

    /**
     * Waits if necessary for the remote to settle the sent delivery unless it has
     * either already been settled or the original delivery was sent settled in which
     * case the remote will not send a {@link Disposition} back.
     *
     * @return this {@link StreamSenderMessage} instance.
     *
     * @throws ClientException if an error occurs while awaiting the remote settlement.
     */
    StreamSenderMessage awaitSettlement() throws ClientException;

    /**
     * Waits if necessary for the remote to settle the sent delivery unless it has
     * either already been settled or the original delivery was sent settled in which
     * case the remote will not send a {@link Disposition} back.
     *
     * @param timeout
     *      the maximum time to wait for the remote to settle.
     * @param unit
     *      the time unit of the timeout argument.
     *
     * @return this {@link StreamSenderMessage} instance.
     *
     * @throws ClientException if an error occurs while awaiting the remote settlement.
     */
    StreamSenderMessage awaitSettlement(long timeout, TimeUnit unit) throws ClientException;

}
