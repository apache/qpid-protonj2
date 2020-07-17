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
 * A Message type that allows reading of partial message data from incoming AMQP
 * Transfer frames.
 */
public interface StreamedMessage extends AdvancedMessage<Object> {

    /**
     * @return true if the remote has sent all the message data and no more is possible.
     */
    boolean isComplete();

    /**
     * @return the current number of bytes that are available for reading from this message.
     */
    int available();

    /**
     * Reads up to the size of the given buffer or if the buffer is larger than the currently
     * available bytes reads all available bytes into the given buffer.
     *
     * @param buffer
     *      The buffer to copy the available bytes into.
     *
     * @return this {@link StreamedMessage} instance.
     */
    StreamedMessage readBytes(byte[] buffer);

    /**
     * Reads up to the given length value or if the length is larger than the currently
     * available bytes reads all available bytes into the given buffer.
     *
     * @param buffer
     *      The buffer to copy the available bytes into.
     * @param offset
     *      The offset into the given buffer to start copying available bytes.
     * @param length
     *      The number of bytes that can be read into the gven buffer before stopping.
     *
     * @return this {@link StreamedMessage} instance.
     */
    StreamedMessage readBytes(byte[] buffer, int offset, int length);

}
