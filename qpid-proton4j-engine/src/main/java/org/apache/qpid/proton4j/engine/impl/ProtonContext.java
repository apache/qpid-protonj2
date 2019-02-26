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
package org.apache.qpid.proton4j.engine.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton4j.engine.Context;

/**
 * Proton implementation of a Context object.
 */
public class ProtonContext implements Context {

    private final Map<Object, Object> contextMap = new HashMap<>();

    private Object linkedResource;

    @Override
    public Object getLinkedResource() {
        return linkedResource;
    }

    @Override
    public <T> T getLinkedResource(Class<T> typeClass) {
        return typeClass.cast(linkedResource);
    }

    @Override
    public Object get(String key) {
        return contextMap.get(key);
    }

    @Override
    public <T> T get(String key, Class<T> typeClass) {
        return typeClass.cast(contextMap.get(key));
    }

    @Override
    public void set(String key, Object value) {
        contextMap.put(key, value);
    }

    @Override
    public boolean containsKey(String key) {
        return contextMap.containsKey(key);
    }

    @Override
    public Context clear() {
        linkedResource = null;
        contextMap.clear();

        return this;
    }
}
