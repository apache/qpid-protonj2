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

import org.apache.qpid.protonj2.types.messaging.Section;

/**
 * Specialized {@link OutputStream} instance that allows the contents of an message
 * to be written in multiple write operations.  The bytes written are assumed to have
 * already been validly encoded into AMQP message {@link Section} segments based on the
 * provided message format in the {@link RawOutputStreamOptions}.
 *
 * <pre>
 * Sender sender = session.sender("address");
 * OutputStream outputStream = sender.outputStream(new {@link RawOutputStreamOptions}());
 * ...
 * outputStream.write(payload);
 * outputStream.flush();
 * ...
 * outputStream.write(payload);
 * outputStream.close;
 *
 * </pre>
 */
public abstract class RawOutputStream extends OutputStream {

     protected final RawOutputStreamOptions options;

     /**
      * Creates a new {@link RawOutputStreamOptions} which copies the options instance given.
      *
      * @param options
      *      the {@link RawOutputStreamOptions} to use with this instance.
      */
     public RawOutputStream(RawOutputStreamOptions options) {
         this.options = new RawOutputStreamOptions(options);
     }

     /**
      * Sends all currently buffered message body data over the parent {@link Sender} link.
      *
      * @throws IOException if an error occurs while attempting to write the buffered contents.
      */
     @Override
     public abstract void flush() throws IOException;

     /**
      * Closes the {@link RawOutputStream} performing a final flush of buffered data.
      * <p>
      * The remote peer will be notified that the current transfer is now complete when
      * the {@link #close} method has been called.
      *
      * @throws IOException if an error occurs while attempting to write the final data to the remote.
      */
     @Override
     public abstract void close() throws IOException;

}
