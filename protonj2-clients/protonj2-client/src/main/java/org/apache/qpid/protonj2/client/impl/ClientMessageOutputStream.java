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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;
import org.apache.qpid.protonj2.client.MessageOutputStream;
import org.apache.qpid.protonj2.client.MessageOutputStreamOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Section;

/**
 * Implements the {@link MessageOutputStream} API and manages the chunked writes
 * of AMQP Message body data.
 */
public class ClientMessageOutputStream extends MessageOutputStream {

    private final int MIN_BUFFER_SIZE_LIMIT = 256;

    private final ProtonBuffer buffer;
    private final ClientSender sender;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final MessageOutputStreamMessage message;

    private int bytesWritten;
    private int sendCount;

    public ClientMessageOutputStream(ClientSender sender, MessageOutputStreamOptions options) {
        super(options);

        this.sender = sender;

        final int bufferCapacity;
        if (options.streamBufferLimit() > 0) {
            bufferCapacity = Math.max(MIN_BUFFER_SIZE_LIMIT, options.streamBufferLimit());
        } else {
            bufferCapacity = Math.max(MIN_BUFFER_SIZE_LIMIT, (int) sender.session().connection().getProtonConnection().getMaxFrameSize());
        }

        this.buffer = ProtonByteBufferAllocator.DEFAULT.allocate(bufferCapacity, bufferCapacity);

        // Populate the message with configured sections
        message = new MessageOutputStreamMessage();
        message.header(options.header());
        message.deliveryAnnotations(options.deliveryAnnotations());
        message.messageAnnotations(options.messageAnnotations());
        message.properties(options.properties());
        message.complete(false);
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
            int written = 0;

            while (length > 0) {
                int toWrite = Math.min(length, buffer.getWritableBytes());
                bytesWritten += toWrite;
                buffer.writeBytes(bytes, offset + written, toWrite);
                if (!buffer.isWritable()) {
                    flush();
                }
                length -= toWrite;
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
            doFlushPending(bytesWritten == options.streamSize(), false);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true) && sendCount > 0) {
            if (options.streamSize() > 0 && options.streamSize() != bytesWritten) {
                // Limit was set but user did not write all of it so we must abort
                doFlushPending(false, true);
            } else {
                // Limit not set or was set and user wrote that many bytes so we can complete.
                doFlushPending(true, false);
            }
        }
    }

    //----- Internal implementation

    private void checkOutputLimitReached(int writeSize) throws IOException {
        final int outputLimit = options.streamSize();

        if (message.complete()) {
            throw new IOException("Cannot write to an already completed message output stream");
        }

        if (outputLimit > 0 && (bytesWritten + writeSize) > outputLimit) {
            throw new IOException("Cannot write beyond configured stream output limit");
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

    private void doFlushPending(boolean complete, boolean abort) throws IOException {
        if (!abort) {
            message.complete(complete);
        } else {
            message.abort();
        }

        try {
            sender.send(message);
        } catch (ClientException e) {
            new IOException(e);
        }
    }

    private class MessageOutputStreamMessage extends ClientMessage<byte[]> {

        @Override
        public ProtonBuffer encode() {
            final ProtonCompositeBuffer encodedMessage = new ProtonCompositeBuffer();

            if (sendCount == 0) {
                ProtonBuffer preamble = ProtonByteBufferAllocator.DEFAULT.allocate();

                Header header = message.header();
                DeliveryAnnotations deliveryAnnotations = message.deliveryAnnotations();
                MessageAnnotations messageAnnotations = message.messageAnnotations();
                Properties properties = message.properties();
                ApplicationProperties applicationProperties = message.applicationProperties();

                if (header != null) {
                    ClientMessageSupport.encodeSection(header, preamble);
                }
                if (deliveryAnnotations != null) {
                    ClientMessageSupport.encodeSection(deliveryAnnotations, preamble);
                }
                if (messageAnnotations != null) {
                    ClientMessageSupport.encodeSection(messageAnnotations, preamble);
                }
                if (properties != null) {
                    ClientMessageSupport.encodeSection(properties, preamble);
                }
                if (applicationProperties != null) {
                    ClientMessageSupport.encodeSection(applicationProperties, preamble);
                }

                encodedMessage.append(preamble);
            }

            // When an output limit is set we encode only one data section and then just
            // write the follow on bytes as a raw buffer to complete the encoding of the
            // initial constraints we encoded into the Binary value contained within the
            // Data section.
            if (options.streamSize() > 0) {
                if (sendCount > 0) {
                    encodedMessage.append(buffer.duplicate());
                } else {
                    ProtonBuffer body = ProtonByteBufferAllocator.DEFAULT.allocate();

                    body.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
                    body.writeByte(EncodingCodes.SMALLULONG);
                    body.writeByte(Data.DESCRIPTOR_CODE.byteValue());
                    body.writeByte(EncodingCodes.VBIN32);
                    body.writeInt(options.streamSize());

                    encodedMessage.append(body);
                    encodedMessage.append(buffer.duplicate());
                }
            } else {
                ProtonBuffer dataBuffer = ProtonByteBufferAllocator.DEFAULT.allocate();
                ClientMessageSupport.encodeSection(new Data(new Binary(buffer.duplicate())), dataBuffer);
                encodedMessage.append(dataBuffer);
            }

            if (message.complete()) {
                Footer footer = message.footer();

                if (options.footerFinalizationEvent() != null) {
                    footer = options.footerFinalizationEvent().apply(footer, bytesWritten);
                }

                if (footer != null) {
                    ProtonBuffer footerBuffer = ProtonByteBufferAllocator.DEFAULT.allocate();
                    ClientMessageSupport.encodeSection(footer, footerBuffer);
                    encodedMessage.append(footerBuffer);
                }
            }

            // Reset internal write buffer for next rounds of writes.
            buffer.setWriteIndex(0);

            sendCount++;

            return encodedMessage;
        }
    }
}
