/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.AbstractRichFunction;

/**
 * RichSinkFunction通过继承AbstractRichFunction为实现一个rich SinkFunction
 * 提供基础(AbstractRichFunction提供了一个open/close方法对，以及获取运行时上下文对象手段)。
 * A {@link org.apache.flink.api.common.functions.RichFunction} version of {@link SinkFunction}.
 */
@Public
public abstract class RichSinkFunction<IN> extends AbstractRichFunction implements SinkFunction<IN> {

	private static final long serialVersionUID = 1L;
}
