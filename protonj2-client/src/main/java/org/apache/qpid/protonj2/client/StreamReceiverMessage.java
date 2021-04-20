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

import java.io.InputStream;

import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * A specialized {@link Message} type that represents a streamed delivery possibly
 * spanning many incoming {@link Transfer} frames from the remote peer.  It is possible
 * for various calls in this {@link StreamReceiverMessage} to block while awaiting the
 * receipt of sufficient bytes to provide the result.
 */
public interface StreamReceiverMessage extends AdvancedMessage<InputStream> {

    /**
     * @return the {@link StreamDelivery} that is associated with the life-cycle of this {@link StreamReceiverMessage}
     */
    StreamDelivery delivery();

    /**
     * @return the {@link StreamReceiver} that this context was create under.
     */
    StreamReceiver receiver();

    /**
     * Check if the {@link StreamDelivery} that was assigned to this {@link StreamReceiverMessage} has been
     * marked as aborted by the remote.
     *
     * @return true if this context has been marked as aborted previously.
     */
    boolean aborted();

    /**
     * Check if the {@link StreamDelivery} that was assigned to this {@link StreamReceiverMessage} has been
     * marked as complete by the remote.
     *
     * @return true if this context has been marked as being the complete.
     */
    boolean completed();

}
