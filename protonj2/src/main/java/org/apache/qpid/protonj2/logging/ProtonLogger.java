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
package org.apache.qpid.protonj2.logging;

/**
 * Proton Logger abstraction
 * <p>
 * This interface provides an abstraction to be used around third party
 * Logging frameworks such as slf4j, log4j etc.
 */
public interface ProtonLogger {

    public String getName();

    public boolean isTraceEnabled();

    public void trace(String message);

    public void trace(String message, Object value);

    public void trace(String message, Object value1, Object value2);

    public void trace(String message, Object... arguments);

    public boolean isDebugEnabled();

    public void debug(String message);

    public void debug(String message, Object value);

    public void debug(String message, Object value1, Object value2);

    public void debug(String message, Object... arguments);

    public boolean isInfoEnabled();

    public void info(String message);

    public void info(String message, Object value);

    public void info(String message, Object value1, Object value2);

    public void info(String message, Object... arguments);

    public boolean isWarnEnabled();

    public void warn(String message);

    public void warn(String message, Object value);

    public void warn(String message, Object value1, Object value2);

    public void warn(String message, Object... arguments);

    public boolean isErrorEnabled();

    public void error(String message);

    public void erro(String message, Object value);

    public void error(String message, Object value1, Object value2);

    public void error(String message, Object... arguments);

}
