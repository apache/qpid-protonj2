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
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Streaming Message Tracker object used to operate on and track the state of a streamed message
 * at the remote. The {@link StreamSenderMessage} allows for local settlement and disposition management
 * as well as waiting for remote settlement of a streamed message.
 */
public interface StreamSenderMessage extends AdvancedMessage<OutputStream> {

    /**
     * @return The {@link Tracker} assigned to monitor the life-cycle of this {@link StreamSenderMessage}
     */
    StreamTracker tracker();

    /**
     * @return the {@link Sender} that was used to send the delivery that is being tracked.
     */
    StreamSender sender();

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
    @Override
    StreamSenderMessage messageFormat(int messageFormat) throws ClientException;

    /**
     * Marks the currently streaming message as being complete.
     * <p>
     * Marking a message as complete finalizes the streaming send operation and causes a
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
     * @return this {@link StreamSenderMessage} instance.
     *
     * @throws ClientException if an error occurs while initiating the abort operation.
     */
    StreamSenderMessage abort() throws ClientException;

    /**
     * @return true if this {@link StreamSenderMessage} has been marked as aborted previously.
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
     * to the {@link #rawOutputStream} method.
     *
     * @param options
     *      The stream options to use to configure the returned {@link OutputStream}
     *
     * @return a {@link OutputStream} instance configured using the given options.
     *
     * @throws ClientException if an error occurs while creating the {@link OutputStream}.
     *
     * @see #rawOutputStream
     */
    OutputStream body(OutputStreamOptions options) throws ClientException;

    /**
     * Creates an {@link OutputStream} instance that writes the bytes given without additional
     * encoding or transformation.  Using this stream option disables use of any other {@link Message}
     * APIs and the message transfer is completed upon close of this {@link OutputStream}.
     * <p>
     * The returned {@link OutputStream} can be used to write the payload of an AMQP Message in
     * chunks when the source is not readily available in memory or as part of a larger streams
     * based component.  The source of the bytes written to the {@link OutputStream} should
     * consist of already encoded AMQP {@link Message} data.  For an {@link OutputStream} that
     * performs the encoding of message data refer to the {@link #body(OutputStreamOptions)}
     * method.
     *
     * @return an {@link OutputStream} instance that performs no encoding.
     *
     * @throws ClientException if an error occurs while creating the {@link OutputStream}.
     *
     * @see #body
     * @see #body(OutputStreamOptions)
     */
    OutputStream rawOutputStream() throws ClientException;

}
