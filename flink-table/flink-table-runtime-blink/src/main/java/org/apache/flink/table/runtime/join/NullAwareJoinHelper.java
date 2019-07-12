/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.join;

import org.apache.flink.table.dataformat.BinaryRow;

import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Helper for null aware join.
 * null 感知连接的帮助类
 */
public class NullAwareJoinHelper {

	// 返回需要过滤null的字段的index数组
	public static int[] getNullFilterKeys(boolean[] filterNulls) {
		checkNotNull(filterNulls);
		List<Integer> nullFilterKeyList = new ArrayList<>();
		for (int i = 0; i < filterNulls.length; i++) {
			if (filterNulls[i]) {
				nullFilterKeyList.add(i);
			}
		}
		return ArrayUtils.toPrimitive(nullFilterKeyList.toArray(new Integer[0]));
	}

	// 是否需要过滤掉
	public static boolean shouldFilter(
			boolean nullSafe, boolean filterAllNulls,
			int[] nullFilterKeys, BinaryRow key) {
		// is not null safe, return false if any null exists.
		// 如果任何null key存在，返回false
		return !nullSafe && (filterAllNulls ? key.anyNull() : key.anyNull(nullFilterKeys));
	}
}
