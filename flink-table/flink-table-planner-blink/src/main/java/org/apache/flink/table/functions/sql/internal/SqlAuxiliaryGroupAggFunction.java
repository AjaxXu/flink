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

package org.apache.flink.table.functions.sql.internal;

import org.apache.flink.annotation.Internal;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * An internal {@link SqlAggFunction} to represents auxiliary group keys
 * which will not be computed as key and does not also affect the correctness of the final result.
 */
@Internal
public class SqlAuxiliaryGroupAggFunction extends SqlAggFunction {

	public SqlAuxiliaryGroupAggFunction() {
		super("AUXILIARY_GROUP",
				null,
				SqlKind.OTHER_FUNCTION,
				ReturnTypes.ARG0,
				null,
				OperandTypes.ANY,
				SqlFunctionCategory.SYSTEM,
				false,
				false);
	}
}
