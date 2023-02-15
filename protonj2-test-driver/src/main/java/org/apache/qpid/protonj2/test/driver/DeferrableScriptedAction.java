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

package org.apache.qpid.protonj2.test.driver;

/**
 * A Type of scripted action whose output can be deferred until the next
 * {@link ScriptedAction} that writes data which is not deferred.
 */
public interface DeferrableScriptedAction extends ScriptedAction {

    /**
     * Configures this action such that any output from the action is
     * deferred until the next action that executes without the defer
     * flag set at which time all deferred data will be written along
     * with the output from the non-deferred action's data.
     * <p>
     * Care should be take when deferring output to ensure that at some
     * point one final non-deferred action is queued or triggered so that
     * tests do not hang waiting for data that has been forever deferred.
     *
     * @return this deferrable scripted action for chaining.
     */
    DeferrableScriptedAction deferred();

    /**
     * @return true if this action's outputs are deferred.
     */
    boolean isDeffered();

}
