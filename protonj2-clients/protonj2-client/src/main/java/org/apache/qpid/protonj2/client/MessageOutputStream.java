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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Section;

/**
 * Specialized {@link OutputStream} instance that allows the body of a message
 * to be written in multiple write operations.
 *
 * <pre>
 * Sender sender = session.sender("address");
 * OutputStream outputStream = sender.outputStream(new MessageOutputStreamOptions());
 * ...
 * outputStream.write(payload);
 * outputStream.flush();
 * ...
 * outputStream.write(payload);
 * outputStream.close;
 *
 * </pre>
 */
public abstract class MessageOutputStream extends OutputStream {

    protected final MessageOutputStreamOptions options;

    /**
     * Creates a new {@link MessageOutputStream} which copies the options instance given.
     *
     * @param options
     *      the {@link MessageOutputStreamOptions} to use with this instance.
     */
    public MessageOutputStream(MessageOutputStreamOptions options) {
        this.options = new MessageOutputStreamOptions(options);
    }

    /**
     * Encodes and sends all currently buffered message body data as an AMQP {@link Data}
     * section, subsequent buffered data will be encoded a follow-on data section on the
     * next {@link #flush()} call unless the stream was configured with an output limit in
     * which cases message body data is streamed as a single AMQP {@link Data} section.
     * <p>
     * If the message has not been previously written then the optional message {@link Section}
     * values from the {@link MessageOutputStreamOptions} will also be encoded except for the
     * message {@link Footer} which is not written until the {@link MessageOutputStream} is
     * closed.
     *
     * @throws IOException if an error occurs while attempting to write the buffered contents.
     */
    @Override
    public abstract void flush() throws IOException;

    /**
     * Closes the {@link MessageOutputStream} performing a final flush of buffered data and
     * writes the AMQP {@link Footer} section if one is provided.  If an output limit was
     * configured in the {@link MessageOutputStreamOptions} and the limit value has not yet
     * been written the ongoing AMQP {@link Tracker} that comprises this message stream is
     * aborted as a partially written message would be invalid on the remote peer.
     */
    @Override
    public abstract void close() throws IOException;

}
