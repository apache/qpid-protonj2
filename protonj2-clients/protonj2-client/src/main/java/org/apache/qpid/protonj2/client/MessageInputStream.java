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
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Specialized {@link OutputStream} instance that allows the body of a message
 * to be written in multiple write operations.
 *
 * <pre>
 * Receiver receiver = session.receiver("address");
 * InputStream inputStream = receiver.inputStream(new MessageOutputStreamOptions());
 * ...
 * inputStream.read(buffer);
 * ...
 * inputStream.read(buffer);
 * inputStream.close;
 *
 * </pre>
 */
public abstract class MessageInputStream extends InputStream {

    public class MessageInputStreamOptions {

    }

    private final MessageInputStreamOptions options;

    public MessageInputStream(MessageInputStreamOptions options) {
        this.options = options;
    }

    public MessageInputStreamOptions options() {
        return options;
    }

    @Override
    public int read() throws IOException {
        return 0;
    }

    @Override
    public int read(byte b[]) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        return -1;
    }

    @Override
    public long skip(long n) throws IOException {
        return 0;
    }

    @Override
    public int available() throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

}
