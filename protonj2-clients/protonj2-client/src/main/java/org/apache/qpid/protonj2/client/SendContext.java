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
import org.apache.qpid.protonj2.types.messaging.Data;
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
     * Returns the configured message format value that will be set on the first outgoing
     * AMQP {@link Transfer} frame for the delivery that comprises this streamed message.
     *
     * @return the configured message format that will be sent.
     */
    int messageFormat();

    /**
     * @return the {@link Sender} that this context is operating within.
     */
    Sender sender();

    /**
     * Following the first send of an {@link AdvancedMessage} for this {@link SendContext}
     * the context will have an assigned {@link Tracker} that can be used to monitor the
     * remote state of the delivery that comprises the multiple framed message transfer.
     *
     * @return the {@link Tracker} instance assigned to this {@link SendContext}
     */
    Tracker tracker();

    /**
     * Writes the given {@link Section} into the {@link SendContext}.  The the data written we
     * be sent to the remote peer when this {@link SendContext} is flushed or the configured send
     * buffer limit is reached.
     *
     * @param section
     *      The {@link Section} instance to be written into this send context..
     *
     * @return this {@link SendContext} instance.
     *
     * @throws ClientException if an error occurs while attempting to write the section.
     */
    SendContext write(Section<?> section) throws ClientException;

    /**
     * Sends all currently buffered message body data over the parent {@link Sender} link.
     *
     * @return this {@link SendContext} instance.
     *
     * @throws ClientException if an error occurs while attempting to write the buffered contents.
     */
    SendContext flush() throws ClientException;

    /**
     * Marks the currently streaming message as being complete.
     * <p>
     * Marking a message as complete finalizes the {@link SendContext} and causes a
     * final {@link Transfer} frame to be sent to the remote indicating that the ongoing
     * streaming delivery is done and no more message data will arrive.
     *
     * @return this {@link SendContext} instance.
     *
     * @throws ClientException if an error occurs while initiating the completion operation.
     */
    SendContext complete() throws ClientException;

    /**
     * @return true if this message has been marked as being complete.
     */
    boolean completed();

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
     * @return this {@link SendContext} instance.
     *
     * @throws ClientException if an error occurs while initiating the abort operation.
     */
    SendContext abort() throws ClientException;

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

}
