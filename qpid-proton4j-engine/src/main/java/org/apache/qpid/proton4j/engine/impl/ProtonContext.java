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

    private Map<Object, Object> contextMap;
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
        return contextMap == null ? null : contextMap.get(key);
    }

    @Override
    public <T> T get(String key, Class<T> typeClass) {
        return contextMap == null ? null : typeClass.cast(contextMap.get(key));
    }

    @Override
    public ProtonContext set(String key, Object value) {
        safeGetContextMap().put(key, value);
        return this;
    }

    @Override
    public boolean containsKey(String key) {
        return contextMap == null ? false : contextMap.containsKey(key);
    }

    @Override
    public Context clear() {
        linkedResource = null;
        if (contextMap != null) {
            contextMap.clear();
        }

        return this;
    }

    private Map<Object, Object> safeGetContextMap() {
        if (contextMap == null) {
            contextMap = new HashMap<>();
        }

        return contextMap;
    }
}
