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

package org.apache.flink.runtime.jobmanager.scheduler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

/**
 * CoLocationGroup 是在 SlotSharingGroup 的基础上的硬限制，它限定了 CoLocationGroup 中的 JobVertex 中的 sub task 运行必须是一一对应的,
 * 且必须运行在相同的TaskManager上：假如 CoLocationGrou 限定了 JobVertex A 和 B ，那么 A 的编号为 i 的 sub task 必须和 B 的编号为 i 的 sub task 跑在一起。
 * A Co-location group is a group of JobVertices, where the <i>i-th</i> subtask of one vertex
 * has to be executed on the same TaskManager as the <i>i-th</i> subtask of all
 * other JobVertices in the same group.
 *
 * The co-location group用于例如确保第i个子任务的迭代头和迭代尾被调度到相同的TaskManager。
 * <p>The co-location group is used for example to make sure that the i-th subtasks for iteration
 * head and iteration tail are scheduled to the same TaskManager.</p>
 */
public class CoLocationGroup implements java.io.Serializable {
	
	private static final long serialVersionUID = -2605819490401895297L;


	/** The ID that describes the slot co-location-constraint as a group */ 
	private final AbstractID id = new AbstractID();
	
	/** The vertices participating in the co-location group */
	private final List<JobVertex> vertices = new ArrayList<JobVertex>();
	
	/** The constraints, which hold the shared slots for the co-located operators */
	private transient ArrayList<CoLocationConstraint> constraints;
	
	// --------------------------------------------------------------------------------------------
	
	public CoLocationGroup() {}
	
	public CoLocationGroup(JobVertex... vertices) {
		this.vertices.addAll(Arrays.asList(vertices));
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void addVertex(JobVertex vertex) {
		Preconditions.checkNotNull(vertex);
		this.vertices.add(vertex);
	}
	
	public void mergeInto(CoLocationGroup other) {
		Preconditions.checkNotNull(other);
		
		for (JobVertex v : this.vertices) {
			v.updateCoLocationGroup(other);
		}
		
		// move vertex membership
		other.vertices.addAll(this.vertices);
		this.vertices.clear();
	}
	
	// --------------------------------------------------------------------------------------------
	// 获取位置限制
	public CoLocationConstraint getLocationConstraint(int subtask) {
		ensureConstraints(subtask + 1);
		return constraints.get(subtask);
	}
	
	private void ensureConstraints(int num) {
		if (constraints == null) {
			constraints = new ArrayList<CoLocationConstraint>(num);
		} else {
			constraints.ensureCapacity(num);
		}
		
		if (num > constraints.size()) {
			constraints.ensureCapacity(num);
			for (int i = constraints.size(); i < num; i++) {
				constraints.add(new CoLocationConstraint(this));
			}
		}
	}

	/**
	 * Gets the ID that identifies this co-location group.
	 * 
	 * @return The ID that identifies this co-location group.
	 */
	public AbstractID getId() {
		return id;
	}

	/**
	 * Resets this co-location group, meaning that future calls to {@link #getLocationConstraint(int)}
	 * will give out new CoLocationConstraints.
	 * 
	 * <p>This method can only be called when no tasks from any of the CoLocationConstraints are
	 * executed any more.</p>
	 */
	public void resetConstraints() {
		for (CoLocationConstraint c : this.constraints) {
			if (c.isAssignedAndAlive()) {
				throw new IllegalStateException(
						"Cannot reset co-location group: some constraints still have live tasks");
			}
		}
		this.constraints.clear();
	}
}
