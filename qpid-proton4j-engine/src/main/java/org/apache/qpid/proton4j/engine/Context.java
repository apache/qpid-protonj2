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

/**
 * Context API used to associate specific context with AMQP Resources
 */
public interface Context {

    /**
     * Links a given resource to this Context.
     *
     * @param resource
     *      The resource to link to this {@link Context}.
     */
    void setLinkedResource(Object resource);

    /**
     * @return the user set linked resource for this Context instance.
     */
    Object getLinkedResource();

    /**
     * Gets the linked resource (if set) and returns it using the type information
     * provided to cast the returned value.
     *
     * @param <T> The type to cast the linked resource to if one is set.
     * @param typeClass the type's Class which is used for casting the returned value.
     *
     * @return the user set linked resource for this Context instance.
     */
    <T> T getLinkedResource(Class<T> typeClass);

    /**
     * Gets the user set context value that is associated with the given key, or null
     * if no data is mapped to the key.
     *
     * @param key
     *      The key to use to lookup the mapped data.
     *
     * @return the object associated with the given key in this Context.
     */
    Object get(String key);

    /**
     * Gets the user set context value that is associated with the given key, or null
     * if no data is mapped to the key.
     *
     * @param <T> The type to cast the context mapped value to if one is set.
     *
     * @param key
     *      The key to use to lookup the mapped data.
     * @param typeClass
     *      The Class that will be used when casting the returned context mapped object.
     *
     * @return the object associated with the given key in this Context.
     */
    <T> T get(String key, Class<T> typeClass);

    /**
     * Maps a given object to the given key in this context instance.
     *
     * @param <T> The type of the value being set
     *
     * @param key
     *      The key to assign the value to
     * @param value
     *      The value to map to the given key.
     *
     * @return this Context instance.
     */
    <T> Context set(String key, T value);

    /**
     * Checks if the given key has a value mapped to it in this context.
     *
     * @param key
     *      The key to search for a mapping to in this context.
     *
     * @return true if there is a value mapped to the given key in this context.
     */
    boolean containsKey(String key);

    /**
     * @return this context with all mapped values and the linked resource cleared.
     */
    Context clear();

}
