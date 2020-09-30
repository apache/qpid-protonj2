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

import org.apache.qpid.protonj2.client.exceptions.ClientException;

/**
 * Sending link implementation that allows sending of large message payload data in
 * multiple transfers to reduce memory overhead of large message sends.
 */
public interface StreamSender extends Sender {

    /**
     * Creates and returns a new {@link StreamSenderMessage} that can be used by the caller to perform
     * multiple sends of custom encoded messages or perform chucked large message transfers to the
     * remote as part of a streaming send operation.
     *
     * @return a new {@link StreamSenderMessage} that can be used to stream message data to the remote.
     *
     * @throws ClientException if an error occurs while initiating a new streaming send tracker.
     */
    StreamSenderMessage beginMessage() throws ClientException;

}
