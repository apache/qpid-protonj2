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
package org.apache.qpid.proton4j.engine;

import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;

/**
 * Pipeline of handlers for Engine work.
 */
public interface EnginePipeline {

    /**
     * @return the {@link Engine} that this pipeline is linked to.
     */
    Engine engine();

    //----- Pipeline management ----------------------------------------------//

    /**
     * Adds the given handler to the front of the pipeline with the given name stored for
     * later lookup or remove operations.  It is not mandatory that each handler have unique
     * names although if handlers do share a name the {@link EnginePipeline#remove(String)}
     * method will only remove them one at a time starting from the first in the pipeline.
     *
     * @param name
     *      The name to assign to the handler
     * @param handler
     *      The {@link EngineHandler} to add into the pipeline.
     *
     * @return this {@link EnginePipeline}.
     *
     * @throws IllegalArgumentException if name is null or empty or the handler is null
     */
    EnginePipeline addFirst(String name, EngineHandler handler);

    /**
     * Adds the given handler to the end of the pipeline with the given name stored for
     * later lookup or remove operations.  It is not mandatory that each handler have unique
     * names although if handlers do share a name the {@link EnginePipeline#remove(String)}
     * method will only remove them one at a time starting from the first in the pipeline.
     *
     * @param name
     *      The name to assign to the handler
     * @param handler
     *      The {@link EngineHandler} to add into the pipeline.
     *
     * @return this {@link EnginePipeline}.
     *
     * @throws IllegalArgumentException if name is null or empty or the handler is null
     */
    EnginePipeline addLast(String name, EngineHandler handler);

    /**
     * Removes the first {@link EngineHandler} in the pipeline.
     *
     * @return this {@link EnginePipeline}.
     */
    EnginePipeline removeFirst();

    /**
     * Removes the last {@link EngineHandler} in the pipeline.
     *
     * @return this {@link EnginePipeline}.
     */
    EnginePipeline removeLast();

    /**
     * Removes the first handler that is found in the pipeline that matches the given name.
     *
     * @param name
     *      The name to search for in the pipeline moving from first to last.
     *
     * @return this {@link EnginePipeline}.
     */
    EnginePipeline remove(String name);

    /**
     * Removes the given {@link EngineHandler} from the pipeline if present.
     *
     * @param handler
     *      The handler instance to remove if contained in the pipeline.
     *
     * @return this {@link EnginePipeline}.
     */
    EnginePipeline remove(EngineHandler handler);

    /**
     * Finds and returns first handler that is found in the pipeline that matches the given name.
     *
     * @param name
     *      The name to search for in the pipeline moving from first to last.
     *
     * @return the {@link EngineHandler} that matches the given name or null if none in the pipeline.
     */
    EngineHandler find(String name);

    /**
     * @return the first {@link EngineHandler} in the pipeline or null if empty.
     */
    EngineHandler first();

    /**
     * @return the last {@link EngineHandler} in the pipeline or null if empty.
     */
    EngineHandler last();

    /**
     * @return the first {@link EngineHandlerContext} in the pipeline or null if empty.
     */
    EngineHandlerContext firstContext();

    /**
     * @return the last {@link EngineHandlerContext} in the pipeline or null if empty.
     */
    EngineHandlerContext lastContext();

    //----- Event triggers ---------------------------------------------------//

    /**
     * Fires an engine starting event to each handler in the pipeline.  Should be used
     * by the engine implementation to signal its handlers that they should initialize.
     *
     * @return this {@link EnginePipeline}.
     */
    EnginePipeline fireEngineStarting();

    /**
     * Fires an engine state changed event to each handler in the pipeline.  Should be used
     * by the engine implementation to signal its handlers that they should respond to the new
     * engine state, e.g. the engine failed or was shutdown.
     *
     * @return this {@link EnginePipeline}.
     */
    EnginePipeline fireEngineStateChanged();

    /**
     * Fires a read event consisting of the given {@link ProtonBuffer} into the pipeline starting
     * from the last {@link EngineHandler} in the pipeline and moving through each until the incoming
     * work is fully processed.  If the read events reaches the head of the pipeline and is not handled
     * by any handler an error is thrown and the engine should enter the failed state.
     *
     * @param input
     *      The {@link ProtonBuffer} to inject into the engine pipeline.
     *
     * @return this {@link EnginePipeline}.
     */
    EnginePipeline fireRead(ProtonBuffer input);

    /**
     * Fires a read event consisting of the given {@link HeaderFrame} into the pipeline starting
     * from the last {@link EngineHandler} in the pipeline and moving through each until the incoming
     * work is fully processed.  If the read events reaches the head of the pipeline and is not handled
     * by any handler an error is thrown and the engine should enter the failed state.
     *
     * @param header
     *      The {@link HeaderFrame} to inject into the engine pipeline.
     *
     * @return this {@link EnginePipeline}.
     */
    EnginePipeline fireRead(HeaderFrame header);

    /**
     * Fires a read event consisting of the given {@link SaslFrame} into the pipeline starting
     * from the last {@link EngineHandler} in the pipeline and moving through each until the incoming
     * work is fully processed.  If the read events reaches the head of the pipeline and is not handled
     * by any handler an error is thrown and the engine should enter the failed state.
     *
     * @param frame
     *      The {@link SaslFrame} to inject into the engine pipeline.
     *
     * @return this {@link EnginePipeline}.
     */
    EnginePipeline fireRead(SaslFrame frame);

    /**
     * Fires a read event consisting of the given {@link ProtocolFrame} into the pipeline starting
     * from the last {@link EngineHandler} in the pipeline and moving through each until the incoming
     * work is fully processed.  If the read events reaches the head of the pipeline and is not handled
     * by any handler an error is thrown and the engine should enter the failed state.
     *
     * @param frame
     *      The {@link ProtocolFrame} to inject into the engine pipeline.
     *
     * @return this {@link EnginePipeline}.
     */
    EnginePipeline fireRead(ProtocolFrame frame);

    /**
     * Fires a write event consisting of the given {@link AMQPHeader} into the pipeline starting
     * from the first {@link EngineHandler} in the pipeline and moving through each until the outgoing
     * work is fully processed.  If the write events reaches the tail of the pipeline and is not handled
     * by any handler an error is thrown and the engine should enter the failed state.
     *
     * It is expected that after the fire write method returns the given {@link AMQPHeader} will have been
     * written or if held for later the object must be copied.
     *
     * @param header
     *      The {@link AMQPHeader} to inject into the engine pipeline.
     *
     * @return this {@link EnginePipeline}.
     */
    EnginePipeline fireWrite(AMQPHeader header);

    /**
     * Fires a write event consisting of the given {@link Performative} into the pipeline starting
     * from the first {@link EngineHandler} in the pipeline and moving through each until the outgoing
     * work is fully processed.  If the write events reaches the tail of the pipeline and is not handled
     * by any handler an error is thrown and the engine should enter the failed state.
     *
     * It is expected that after the fire write method returns the given {@link Performative} will have been
     * written or if held for later the object must be copied.
     *
     * When the payload given exceeds the maximum allowed frame size when encoded into an outbound frame the
     * amount written will only be the allowable portion and the payload to large callback will be invoked and
     * the frame re-encoded to allow for updates to the given performative.
     *
     * @param performative
     *      The {@link Performative} to inject into the engine pipeline
     * @param channel
     *      The channel that should be assigned to the output frame.
     * @param payload
     *      The payload that should be encoded into the output frame.
     * @param payloadToLarge
     *      A handler that should be invoked if the payload given would exceed the max frame size limit.
     *
     * @return this {@link EnginePipeline}.
     */
    EnginePipeline fireWrite(Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge);

    /**
     * Fires a write event consisting of the given {@link SaslPerformative} into the pipeline starting
     * from the first {@link EngineHandler} in the pipeline and moving through each until the outgoing
     * work is fully processed.  If the write events reaches the tail of the pipeline and is not handled
     * by any handler an error is thrown and the engine should enter the failed state.
     *
     * It is expected that after the fire write method returns the given {@link SaslPerformative} will have been
     * written or if held for later the object must be copied.
     *
     * @param performative
     *      The {@link SaslPerformative} to inject into the engine pipeline.
     *
     * @return this {@link EnginePipeline}.
     */
    EnginePipeline fireWrite(SaslPerformative performative);

    /**
     * Fires a write event consisting of the given {@link ProtonBuffer} into the pipeline starting
     * from the first {@link EngineHandler} in the pipeline and moving through each until the outgoing
     * work is fully processed.  If the write events reaches the tail of the pipeline and is not handled
     * by any handler an error is thrown and the engine should enter the failed state.
     *
     * @param buffer
     *      The {@link ProtonBuffer} to inject into the engine pipeline.
     *
     * @return this {@link EnginePipeline}.
     */
    EnginePipeline fireWrite(ProtonBuffer buffer);

    /**
     * Fires an engine failed event into each {@link EngineHandler} in the pipeline indicating
     * that the engine is now failed and should not accept or produce new work.
     *
     * @param failure
     *      The cause of the engine failure.
     *
     * @return this {@link EnginePipeline}.
     */
    EnginePipeline fireFailed(EngineFailedException failure);

}
