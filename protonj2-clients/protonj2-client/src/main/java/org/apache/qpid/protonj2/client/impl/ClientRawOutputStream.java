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
package org.apache.qpid.protonj2.client.impl;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.client.MessageOutputStream;
import org.apache.qpid.protonj2.client.MessageOutputStreamOptions;
import org.apache.qpid.protonj2.client.RawOutputStream;
import org.apache.qpid.protonj2.client.RawOutputStreamOptions;
import org.apache.qpid.protonj2.client.SendContext;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Section;

/**
 * Implements the {@link MessageOutputStream} API and manages the chunked writes
 * of AMQP Message body data.
 */
public class ClientRawOutputStream extends RawOutputStream {

    /**
     * Defines the default minimum size that the {@link ClientRawOutputStream} will allocate
     * which drives the interval of stream auto flushing of written data.
     */
    public static final int MIN_BUFFER_SIZE_LIMIT = 256;

    private final ProtonBuffer buffer;
    private final ClientSender sender;
    private final SendContext sendContext;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final RawOutputStreamMessage message;

    private int bytesWritten;

    public ClientRawOutputStream(ClientSender sender, RawOutputStreamOptions options) {
        super(options);

        this.sender = sender;
        this.sendContext = sender.newSendContext();

        final int bufferCapacity;
        if (options.streamBufferLimit() > 0) {
            bufferCapacity = Math.max(MIN_BUFFER_SIZE_LIMIT, options.streamBufferLimit());
        } else {
            bufferCapacity = Math.max(MIN_BUFFER_SIZE_LIMIT, (int) sender.session().connection().getProtonConnection().getMaxFrameSize());
        }

        this.buffer = ProtonByteBufferAllocator.DEFAULT.allocate(bufferCapacity, bufferCapacity);

        // Populate the message with configured sections
        message = new RawOutputStreamMessage();
    }

    @Override
    public void write(int value) throws IOException {
        checkClosed();
        checkOutputLimitReached(1);
        buffer.writeByte(value);
        if (!buffer.isWritable()) {
            flush();
        }
        bytesWritten++;
    }

    @Override
    public void write(byte bytes[]) throws IOException {
        write(bytes, 0, bytes.length);
    }

    @Override
    public void write(byte bytes[], int offset, int length) throws IOException {
        checkClosed();
        checkOutputLimitReached(length);
        if (buffer.getWritableBytes() >= length) {
            buffer.writeBytes(bytes, offset, length);
            bytesWritten += length;
            if (!buffer.isWritable()) {
                flush();
            }
        } else {
            int remaining = length;

            while (remaining > 0) {
                int toWrite = Math.min(length, buffer.getWritableBytes());
                bytesWritten += toWrite;
                buffer.writeBytes(bytes, offset + (length - remaining), toWrite);
                if (!buffer.isWritable()) {
                    flush();
                }
                remaining -= toWrite;
            }
        }
    }

    /**
     * Encodes and sends all currently buffered message body data as an AMQP
     * {@link Data} section, subsequent buffered data will be encoded a follow
     * on data section on the next flush call.
     * <p>
     * If the message has not been previously written then the optional message
     * {@link Section} values from the {@link MessageOutputStreamOptions} will also be encoded
     * except for the message {@link Footer} which is not written until the
     * {@link MessageOutputStream} is closed.
     *
     * @throws IOException if an error occurs while attempting to write the buffered contents.
     */
    @Override
    public void flush() throws IOException {
        checkClosed();

        if (buffer.getReadableBytes() > 0) {
            doFlushPending(false);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true) && bytesWritten > 0 && !sendContext.completed()) {
            doFlushPending(true);
        }
    }

    //----- Internal implementation

    private void checkOutputLimitReached(int writeSize) throws IOException {
        if (sendContext.completed()) {
            throw new IOException("Cannot write to an already completed message output stream");
        }
    }

    private void checkClosed() throws IOException {
        if (closed.get()) {
            throw new IOException("The OutputStream has already been closed.");
        }

        if (sender.isClosed()) {
            throw new IOException("The parent Sender instance has already been closed.");
        }
    }

    private void doFlushPending(boolean complete) throws IOException {
        try {
            if (complete) {
                sendContext.complete(message);
                sendContext.tracker().acknowledgeFuture().get();
                sendContext.tracker().settle();
            } else {
                sendContext.send(message);
            }
        } catch (ClientException | InterruptedException | ExecutionException e) {
            new IOException(e);
        }
    }

    private class RawOutputStreamMessage extends ClientMessage<byte[]> {

        @Override
        public ProtonBuffer encode() {
            final ProtonBuffer encodedMessage;

            if (buffer.isReadable()) {
                encodedMessage = buffer.duplicate();
                // Reset internal write buffer for next rounds of writes.
                buffer.setIndex(0, 0);
            } else {
                encodedMessage = null;
            }

            return encodedMessage;
        }
    }
}
