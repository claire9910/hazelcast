/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.executor;

/**
 * Represents abstract execution payload;
 */
public interface Payload {
    /**
     * Set marker to payload if last processing operation contained useful operations;
     *
     * @param activity - true - it there were useful operations, false - otherwise;
     */
    void set(boolean activity);

    /**
     * @return -true if there were useful operations, false otherwise;
     */
    boolean produced();
}
