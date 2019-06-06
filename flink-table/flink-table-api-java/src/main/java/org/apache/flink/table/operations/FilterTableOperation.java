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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.Expression;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Filters out rows of underlying relational operation that do not match given condition.
 */
@Internal
public class FilterTableOperation extends TableOperation {

	private final Expression condition;
	private final TableOperation child;

	public FilterTableOperation(Expression condition, TableOperation child) {
		this.condition = condition;
		this.child = child;
	}

	public Expression getCondition() {
		return condition;
	}

	@Override
	public TableSchema getTableSchema() {
		return child.getTableSchema();
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("condition", condition);

		return formatWithChildren("Filter", args);
	}

	@Override
	public List<TableOperation> getChildren() {
		return Collections.singletonList(child);
	}

	@Override
	public <T> T accept(TableOperationVisitor<T> visitor) {
		return visitor.visitFilter(this);
	}

}
