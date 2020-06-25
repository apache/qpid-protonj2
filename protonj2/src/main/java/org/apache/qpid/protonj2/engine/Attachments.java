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
package org.apache.qpid.protonj2.engine;

/**
 * Attachments API used to associate specific data with AMQP Resources
 */
public interface Attachments {

    /**
     * Gets the user attached value that is associated with the given key, or null
     * if no data is mapped to the key.
     *
     * @param <T> The type to cast the attached mapped value to if one is set.
     *
     * @param key
     *      The key to use to lookup the mapped data.
     *
     * @return the object associated with the given key in this {@link Attachments} instance.
     */
    <T> T get(String key);

    /**
     * Gets the user set {@link Attachments} value that is associated with the given key, or null
     * if no data is mapped to the key.
     *
     * @param <T> The type to cast the attached mapped value to if one is set.
     *
     * @param key
     *      The key to use to lookup the mapped data.
     * @param typeClass
     *      The Class that will be used when casting the returned mapped object.
     *
     * @return the object associated with the given key in this {@link Attachments} instance.
     */
    <T> T get(String key, Class<T> typeClass);

    /**
     * Maps a given object to the given key in this {@link Attachments} instance.
     *
     * @param <T> The type of the value being set
     *
     * @param key
     *      The key to assign the value to
     * @param value
     *      The value to map to the given key.
     *
     * @return this {@link Attachments} instance.
     */
    <T> Attachments set(String key, T value);

    /**
     * Checks if the given key has a value mapped to it in this {@link Attachments} instance.
     *
     * @param key
     *      The key to search for a mapping to in this {@link Attachments} instance.
     *
     * @return true if there is a value mapped to the given key in this {@link Attachments} instance.
     */
    boolean containsKey(String key);

    /**
     * @return this {@link Attachments} instance with all mapped values and the linked resource cleared.
     */
    Attachments clear();

}
