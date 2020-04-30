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
package org.apache.qpid.proton4j.engine.util;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.openjdk.jmh.runner.RunnerException;

/**
 * Comparison Benchmark for the Java {@link LinkedHashMap} to compare against internal implementations
 */
public class LinkedHashMapBenchmark extends MapBenchmarkBase {

    public static void main(String[] args) throws RunnerException {
        runBenchmark(LinkedHashMapBenchmark.class);
    }

    @Override
    protected Map<UnsignedInteger, String> createMap() {
        return new LinkedHashMap<>();
    }
}
