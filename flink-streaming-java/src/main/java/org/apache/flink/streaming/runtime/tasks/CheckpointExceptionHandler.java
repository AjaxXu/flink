/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;

/**
 * 处理发生在checkpointing阶段发生的异常的处理器.处理器可以拒绝或者重新抛出异常
 * Handler for exceptions that happen on checkpointing. The handler can reject and rethrow the offered exceptions.
 */
public interface CheckpointExceptionHandler {

	/**
	 * Offers the exception for handling. If the exception cannot be handled from this instance, it is rethrown.
	 *
	 * @param checkpointMetaData metadata for the checkpoint for which the exception occurred.
	 * @param exception  the exception to handle.
	 * @throws Exception rethrows the exception if it cannot be handled.
	 */
	void tryHandleCheckpointException(CheckpointMetaData checkpointMetaData, Exception exception) throws Exception;
}
