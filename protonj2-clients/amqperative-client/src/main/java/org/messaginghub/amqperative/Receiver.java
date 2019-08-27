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
package org.messaginghub.amqperative;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public interface Receiver {

    /**
     * @return the {@link Client} instance that holds this session's {@link Receiver}
     */
    Client getClient();

    /**
     * @return the {@link Session} that created and holds this {@link Receiver}.
     */
    Session getSession();

    // Waits forever.
    Delivery receive() throws IllegalStateException;

    // Returns message if there is one, null if not.
    Delivery tryReceive() throws IllegalStateException;

    Delivery receive(long timeout) throws IllegalStateException;
    // TODO: with credit window, above is fine...without, we would need to
    // manage the credit in one of various fashions (or say we dont).

    Future<Receiver> openFuture();

    Future<Receiver> close();

    Future<Receiver> detach();

    // TODO: ideas
    long getQueueSize();

    Receiver onMessage(Consumer<Delivery> handler);

    Receiver onMessage(Consumer<Delivery> handler, ExecutorService executor);

    Receiver addCredit(int credits) throws IllegalStateException;

    Future<Receiver> drainCredit(long timeout) throws IllegalStateException, IllegalArgumentException;

}
