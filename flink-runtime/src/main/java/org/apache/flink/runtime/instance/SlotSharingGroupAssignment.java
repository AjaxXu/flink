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

package org.apache.flink.runtime.instance;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * SlotSharingGroupAssignment管理shared slots集合，它们在{@link org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup}的任务之间共享.
 * The SlotSharingGroupAssignment manages a set of shared slots, which are shared between
 * tasks of a {@link org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup}.
 * 
 * <p>The assignments shares tasks by allowing a shared slot to hold one vertex per
 * JobVertexID. For example, consider a program consisting of job vertices "source", "map",
 * "reduce", and "sink". If the slot sharing group spans all four job vertices, then
 * each shared slot can hold one parallel subtask of the source, the map, the reduce, and the
 * sink vertex. Each shared slot holds the actual subtasks in child slots, which are (at the leaf level),
 * the {@link SimpleSlot}s.</p>
 *
 * 一个例外是co-location-constraints，表示vertex的第i个subtask必须和co-location-constraint的其他vertex的第i个subtask放在一起.
 * 为了管理这种关系，co-location-constraint获得他自己的shared slot
 * <p>An exception are the co-location-constraints, that define that the i-th subtask of one
 * vertex needs to be scheduled strictly together with the i-th subtasks of the vertices
 * that share the co-location-constraint. To manage that, a co-location-constraint gets its
 * own shared slot inside the shared slots of a sharing group.</p>
 * 
 * <p>Consider a job set up like this:</p>
 * 
 * <pre>{@code
 * +-------------- Slot Sharing Group --------------+
 * |                                                |
 * |            +-- Co Location Group --+           |
 * |            |                       |           |
 * |  (source) ---> (head) ---> (tail) ---> (sink)  |
 * |            |                       |           |
 * |            +-----------------------+           |
 * +------------------------------------------------+
 * }</pre>
 * 
 * <p>The slot hierarchy in the slot sharing group will look like the following</p> 
 * 
 * <pre>
 *     Shared(0)(root)
 *        |
 *        +-- Simple(2)(sink)
 *        |
 *        +-- Shared(1)(co-location-group)
 *        |      |
 *        |      +-- Simple(0)(tail)
 *        |      +-- Simple(1)(head)
 *        |
 *        +-- Simple(0)(source)
 * </pre>
 */
public class SlotSharingGroupAssignment {

	private final static Logger LOG = LoggerFactory.getLogger(SlotSharingGroupAssignment.class);

	/** The lock globally guards against concurrent modifications in the data structures */
	private final Object lock = new Object();
	
	/** All slots currently allocated to this sharing group */
	private final Set<SharedSlot> allSlots = new LinkedHashSet<>();

	/** Map<JobVertexId, Map<TaskManager, List<SharedSlot>>>
	 * 资源以 JobVertex 微粒度划分 group，也就是一个 JobVertex 占有一个资源 group
	 * The slots available per vertex type (JobVertexId), keyed by TaskManager, to make them locatable */
	private final Map<AbstractID, Map<ResourceID, List<SharedSlot>>> availableSlotsPerJid = new LinkedHashMap<>();


	// --------------------------------------------------------------------------------------------
	//  Accounting
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the number of slots that are currently governed by this assignment group.
	 * This refers to the slots allocated from an {@link org.apache.flink.runtime.instance.Instance},
	 * and not the sub-slots given out as children of those shared slots.
	 * 
	 * @return The number of resource slots managed by this assignment group.
	 */
	public int getNumberOfSlots() {
		return allSlots.size();
	}

	/**
	 * Gets the number of shared slots into which the given group can place subtasks or 
	 * nested task groups.
	 * 
	 * @param groupId The ID of the group.
	 * @return The number of shared slots available to the given job vertex.
	 */
	public int getNumberOfAvailableSlotsForGroup(AbstractID groupId) {
		synchronized (lock) {
			Map<ResourceID, List<SharedSlot>> available = availableSlotsPerJid.get(groupId);

			if (available != null) {
				Set<SharedSlot> set = new HashSet<SharedSlot>();

				for (List<SharedSlot> list : available.values()) {
					set.addAll(list);
				}

				return set.size();
			}
			else {
				// 如果该JobVertexID没有entry，则该vertex可以把子任务加入任何一个shared slot中
				// if no entry exists for a JobVertexID so far, then the vertex with that ID can
				// add a subtask into each shared slot of this group. Consequently, all
				// of them are available for that JobVertexID.
				return allSlots.size();
			}
		}
	}
	
	// ------------------------------------------------------------------------
	//  Slot allocation
	// ------------------------------------------------------------------------

	// 添加Shared slot到 该assignment 中
	public SimpleSlot addSharedSlotAndAllocateSubSlot(SharedSlot sharedSlot, Locality locality, JobVertexID groupId) {
		return addSharedSlotAndAllocateSubSlot(sharedSlot, locality, groupId, null);
	}

	public SimpleSlot addSharedSlotAndAllocateSubSlot(
			SharedSlot sharedSlot, Locality locality, CoLocationConstraint constraint)
	{
		return addSharedSlotAndAllocateSubSlot(sharedSlot, locality, null, constraint);
	}

	private SimpleSlot addSharedSlotAndAllocateSubSlot(
			SharedSlot sharedSlot, Locality locality, JobVertexID groupId, CoLocationConstraint constraint) {

		// sanity checks
		if (!sharedSlot.isRootAndEmpty()) {
			throw new IllegalArgumentException("The given slot is not an empty root slot.");
		}

		final ResourceID location = sharedSlot.getTaskManagerID();

		synchronized (lock) {
			// early out in case that the slot died (instance disappeared)
			if (!sharedSlot.isAlive()) {
				return null;
			}
			
			// add to the total bookkeeping
			// 把shared slot 加入列表中
			if (!allSlots.add(sharedSlot)) {
				throw new IllegalArgumentException("Slot was already contained in the assignment group");
			}
			
			SimpleSlot subSlot;
			AbstractID groupIdForMap;
					
			if (constraint == null) {
				// allocate us a sub slot to return
				// 从当前的shared slot分配sub slot
				subSlot = sharedSlot.allocateSubSlot(groupId);
				groupIdForMap = groupId;
			}
			else {
				// sanity check
				if (constraint.isAssignedAndAlive()) {
					// constraint 已经有一个alive 的 shared slot，抛出异常
					throw new IllegalStateException(
							"Trying to add a shared slot to a co-location constraint that has a life slot.");
				}
				
				// we need a co-location slot --> a SimpleSlot nested in a SharedSlot to
				//                                host other co-located tasks
				// 分配一个co-location slot
				SharedSlot constraintGroupSlot = sharedSlot.allocateSharedSlot(constraint.getGroupId());
				groupIdForMap = constraint.getGroupId();
				
				if (constraintGroupSlot != null) {
					// the sub-slots in the co-location constraint slot have no own group IDs
					// co-location constraint 里的sub-slots没有自己的 group id（JobVertex）
					subSlot = constraintGroupSlot.allocateSubSlot(null);
					if (subSlot != null) {
						// all went well, we can give the constraint its slot
						// 将分配的shared slot设置到constraint中
						constraint.setSharedSlot(constraintGroupSlot);
						
						// NOTE: Do not lock the location constraint, because we don't yet know whether we will
						// take the slot here
					}
					else {
						// 如果不能分配sub-slot, 则释放该刚分配的shared slot
						// if we could not create a sub slot, release the co-location slot
						// note that this does implicitly release the slot we have just added
						// as well, because we release its last child slot. That is expected
						// and desired.
						constraintGroupSlot.releaseSlot(new FlinkException("Could not create a sub slot in this shared slot."));
					}
				}
				else {
					// this should not happen, as we are under the lock that also
					// guards slot disposals. Keep the check to be on the safe side
					subSlot = null;
				}
			}
			
			if (subSlot != null) {
				// preserve the locality information
				// 保存位置信息
				subSlot.setLocality(locality);
				
				// let the other groups know that this slot exists and that they
				// can place a task into this slot.
				// 让其他group知道该shared slot（最开始的）存在，这样他们可以把task放在该slot上
				boolean entryForNewJidExists = false;

				// 遍历可用的slot，把该shared slot 加入进去
				for (Map.Entry<AbstractID, Map<ResourceID, List<SharedSlot>>> entry : availableSlotsPerJid.entrySet()) {
					// there is already an entry for this groupID
					// 该groupId已经存在一条，则直接跳过.因为已经被该groupId使用了，等release时可以加入
					if (entry.getKey().equals(groupIdForMap)) {
						entryForNewJidExists = true;
						continue;
					}

					Map<ResourceID, List<SharedSlot>> available = entry.getValue();
					putIntoMultiMap(available, location, sharedSlot);
				}

				// make sure an empty entry exists for this group, if no other entry exists
				// 如果不存在则加入一条空的entry
				if (!entryForNewJidExists) {
					availableSlotsPerJid.put(groupIdForMap, new LinkedHashMap<>());
				}

				return subSlot;
			}
			else {
				// if sharedSlot is releases, abort.
				// This should be a rare case, since this method is called with a fresh slot.
				return null;
			}
		}
		// end synchronized (lock)
	}

	/**
	 * 为task vertex 获取一个合适的slot.会根据locationPreferences(位置偏好)先获取
	 * Gets a slot suitable for the given task vertex. This method will prefer slots that are local
	 * (with respect to {@link ExecutionVertex#getPreferredLocationsBasedOnInputs()}), but will return non local
	 * slots if no local slot is available. The method returns null, when this sharing group has
	 * no slot available for the given JobVertexID.
	 *
	 * @param vertexID the vertex id
	 * @param locationPreferences location preferences
	 *
	 * @return A slot to execute the given ExecutionVertex in, or null, if none is available.
	 */
	public SimpleSlot getSlotForTask(JobVertexID vertexID, Iterable<TaskManagerLocation> locationPreferences) {
		synchronized (lock) {
			Tuple2<SharedSlot, Locality> p = getSharedSlotForTask(vertexID, locationPreferences, false);

			if (p != null) {
				SharedSlot ss = p.f0;
				SimpleSlot slot = ss.allocateSubSlot(vertexID);
				// 为slot设置位置信息
				slot.setLocality(p.f1);
				return slot;
			}
			else {
				return null;
			}
		}
	}

	/**
	 * Gets a slot for a task that has a co-location constraint. This method tries to grab
	 * a slot form the location-constraint's shared slot. If that slot has not been initialized,
	 * then the method tries to grab another slot that is available for the location-constraint-group.
	 * 
	 * <p>In cases where the co-location constraint has not yet been initialized with a slot,
	 * or where that slot has been disposed in the meantime, this method tries to allocate a shared
	 * slot for the co-location constraint (inside on of the other available slots).</p>
	 * 
	 * <p>If a suitable shared slot is available, this method allocates a simple slot within that
	 * shared slot and returns it. If no suitable shared slot could be found, this method
	 * returns null.</p>
	 * 
	 * @param constraint The co-location constraint for the placement of the execution vertex.
	 * @param locationPreferences location preferences
	 * 
	 * @return A simple slot allocate within a suitable shared slot, or {@code null}, if no suitable
	 *         shared slot is available.
	 */
	public SimpleSlot getSlotForTask(CoLocationConstraint constraint, Iterable<TaskManagerLocation> locationPreferences) {
		synchronized (lock) {
			// constraint中location已分配且shared slot is alive
			if (constraint.isAssignedAndAlive()) {
				// the shared slot of the co-location group is initialized and set we allocate a sub-slot
				final SharedSlot shared = constraint.getSharedSlot();
				SimpleSlot subslot = shared.allocateSubSlot(null);
				subslot.setLocality(Locality.LOCAL);
				return subslot;
			}
			else if (constraint.isAssigned()) {
				// we had an assignment before.
				// location is assigned but the slot is canceled or released
				SharedSlot previous = constraint.getSharedSlot();
				if (previous == null) {
					throw new IllegalStateException("Bug: Found assigned co-location constraint without a slot.");
				}

				TaskManagerLocation location = previous.getTaskManagerLocation();
				Tuple2<SharedSlot, Locality> p = getSharedSlotForTask(
						constraint.getGroupId(), Collections.singleton(location), true);

				if (p == null) {
					return null;
				}
				else {
					SharedSlot newSharedSlot = p.f0;

					// allocate the co-location group slot inside the shared slot
					// 在根据location得到的shared slot 分配co-location group slot，分配内嵌的shared slot
					SharedSlot constraintGroupSlot = newSharedSlot.allocateSharedSlot(constraint.getGroupId());
					if (constraintGroupSlot != null) {
						constraint.setSharedSlot(constraintGroupSlot);

						// the sub slots in the co location constraint slot have no group that they belong to
						// (other than the co-location-constraint slot)
						SimpleSlot subSlot = constraintGroupSlot.allocateSubSlot(null);
						subSlot.setLocality(Locality.LOCAL);
						return subSlot;
					}
					else {
						// could not allocate the co-location-constraint shared slot
						return null;
					}
				}
			}
			else {
				// 位置限制还没有和shared slot绑定，则不需要根据本地性，获取一个shared slot
				// the location constraint has not been associated with a shared slot, yet.
				// grab a new slot and initialize the constraint with that one.
				// preferred locations are defined by the vertex
				Tuple2<SharedSlot, Locality> p =
						getSharedSlotForTask(constraint.getGroupId(), locationPreferences, false);
				if (p == null) {
					// could not get a shared slot for this co-location-group
					return null;
				}
				else {
					final SharedSlot availableShared = p.f0;
					final Locality l = p.f1;

					// allocate the co-location group slot inside the shared slot
					SharedSlot constraintGroupSlot = availableShared.allocateSharedSlot(constraint.getGroupId());
					
					// IMPORTANT: We do not lock the location, yet, since we cannot be sure that the
					//            caller really sticks with the slot we picked!
					// 重要：我们这里并不lock位置，因为不确定调用者是否坚持选择的slot
					constraint.setSharedSlot(constraintGroupSlot);
					
					// the sub slots in the co location constraint slot have no group that they belong to
					// (other than the co-location-constraint slot)
					SimpleSlot sub = constraintGroupSlot.allocateSubSlot(null);
					sub.setLocality(l);
					return sub;
				}
			}
		}
	}


	public Tuple2<SharedSlot, Locality> getSharedSlotForTask(
			AbstractID groupId,
			Iterable<TaskManagerLocation> preferredLocations,
			boolean localOnly) {
		// check if there is anything at all in this group assignment
		if (allSlots.isEmpty()) {
			return null;
		}

		// get the available slots for the group
		// 获取该groupId(JobVertexID)对应的可用的slots集合
		Map<ResourceID, List<SharedSlot>> slotsForGroup = availableSlotsPerJid.get(groupId);
		
		if (slotsForGroup == null) {
			// we have a new group, so all slots are available
			// 一个新的group，则所有的slots都可用
			slotsForGroup = new LinkedHashMap<>();
			availableSlotsPerJid.put(groupId, slotsForGroup);

			for (SharedSlot availableSlot : allSlots) {
				putIntoMultiMap(slotsForGroup, availableSlot.getTaskManagerID(), availableSlot);
			}
		}
		else if (slotsForGroup.isEmpty()) {
			// the group exists, but nothing is available for that group
			// group 存在，当没有可用的slot，直接返回null
			return null;
		}

		// check whether we can schedule the task to a preferred location
		boolean didNotGetPreferred = false;

		if (preferredLocations != null) {
			for (TaskManagerLocation location : preferredLocations) {

				// set the flag that we failed a preferred location. If one will be found,
				// we return early anyways and skip the flag evaluation
				// 设置没有找到 preferred 位置.如果找到会直接返回，跳过该flag检测
				didNotGetPreferred = true;

				SharedSlot slot = removeFromMultiMap(slotsForGroup, location.getResourceID());
				if (slot != null && slot.isAlive()) {
					return new Tuple2<>(slot, Locality.LOCAL);
				}
			}
		}

		// if we want only local assignments, exit now with a "not found" result
		if (didNotGetPreferred && localOnly) {
			return null;
		}

		Locality locality = didNotGetPreferred ? Locality.NON_LOCAL : Locality.UNCONSTRAINED;

		// schedule the task to any available location
		SharedSlot slot;
		while ((slot = pollFromMultiMap(slotsForGroup)) != null) {
			if (slot.isAlive()) {
				return new Tuple2<>(slot, locality);
			}
		}
		
		// nothing available after all, all slots were dead
		return null;
	}

	// ------------------------------------------------------------------------
	//  Slot releasing
	// ------------------------------------------------------------------------

	/**
	 * 从assignment group中释放.调用顺序为SimpleSlot.releaseSlot -> SharedSlot.releaseChild -> releaseSimpleSlot
	 * Releases the simple slot from the assignment group.
	 * 
	 * @param simpleSlot The SimpleSlot to be released
	 */
	void releaseSimpleSlot(SimpleSlot simpleSlot) {
		synchronized (lock) {
			// try to transition to the CANCELED state. That state marks
			// that the releasing is in progress
			if (simpleSlot.markCancelled()) {

				// sanity checks
				if (simpleSlot.isAlive()) {
					throw new IllegalStateException("slot is still alive");
				}

				// check whether the slot is already released
				if (simpleSlot.markReleased()) {
					LOG.debug("Release simple slot {}.", simpleSlot);

					// JobVertexID, 该simpleSlot被该JobVertex使用
					AbstractID groupID = simpleSlot.getGroupID();
					SharedSlot parent = simpleSlot.getParent();

					// if we have a group ID, then our parent slot is tracked here
					if (groupID != null && !allSlots.contains(parent)) {
						throw new IllegalArgumentException("Slot was not associated with this SlotSharingGroup before.");
					}

					// 从parent的sub slots数组中删除
					int parentRemaining = parent.removeDisposedChildSlot(simpleSlot);

					// Shared slot 还有slot
					if (parentRemaining > 0) {
						// the parent shared slot is still alive. make sure we make it
						// available again to the group of the just released slot

						if (groupID != null) {
							// if we have a group ID, then our parent becomes available
							// for that group again. otherwise, the slot is part of a
							// co-location group and nothing becomes immediately available

							// 该shared slot对JobVertex变得重新可用，因为该shared slot中对应的simple slot已经被删除
							Map<ResourceID, List<SharedSlot>> slotsForJid = availableSlotsPerJid.get(groupID);

							// sanity check
							if (slotsForJid == null) {
								throw new IllegalStateException("Trying to return a slot for group " + groupID +
										" when available slots indicated that all slots were available.");
							}

							putIntoMultiMap(slotsForJid, parent.getTaskManagerID(), parent);
						}
					} else {
						// the parent shared slot is now empty and can be released
						parent.markCancelled();
						internalDisposeEmptySharedSlot(parent);
					}
				}
			}
		}
	}

	/**
	 * Called from {@link org.apache.flink.runtime.instance.SharedSlot#releaseSlot(Throwable)}.
	 * 
	 * @param sharedSlot The slot to be released.
	 */
	void releaseSharedSlot(SharedSlot sharedSlot) {
		synchronized (lock) {
			if (sharedSlot.markCancelled()) {
				// we are releasing this slot
				
				if (sharedSlot.hasChildren()) {
					final FlinkException cause = new FlinkException("Releasing shared slot parent.");
					// by simply releasing all children, we should eventually release this slot.
					Set<Slot> children = sharedSlot.getSubSlots();
					while (children.size() > 0) {
						children.iterator().next().releaseSlot(cause);
					}
				}
				else {
					// if there are no children that trigger the release, we trigger it directly
					internalDisposeEmptySharedSlot(sharedSlot);
				}
			}
		}
	}

	/**
	 * 
	 * <p><b>NOTE: This method must be called from within a scope that holds the lock.</b></p>
	 */
	private void internalDisposeEmptySharedSlot(SharedSlot sharedSlot) {
		// sanity check
		if (sharedSlot.isAlive() || !sharedSlot.getSubSlots().isEmpty()) {
			throw new IllegalArgumentException();
		}
		
		final SharedSlot parent = sharedSlot.getParent();
		final AbstractID groupID = sharedSlot.getGroupID();
		
		// 1) If we do not have a parent, we are a root slot.
		// 2) If we are not a root slot, we are a slot with a groupID and our parent
		//    becomes available for that group
		
		if (parent == null) {
			// root slot, return to the instance.
			// root slot , 返还给instance（代表注册在JobMaster上的TM）
			sharedSlot.getOwner().returnLogicalSlot(sharedSlot);

			// also, make sure we remove this slot from everywhere
			allSlots.remove(sharedSlot);
			removeSlotFromAllEntries(availableSlotsPerJid, sharedSlot);
		}
		else if (groupID != null) {
			// we remove ourselves from our parent slot
			// 从parent slot中删除该 shared slot

			if (sharedSlot.markReleased()) {
				LOG.debug("Internally dispose empty shared slot {}.", sharedSlot);

				int parentRemaining = parent.removeDisposedChildSlot(sharedSlot);
				
				if (parentRemaining > 0) {
					// the parent becomes available for the group again
					Map<ResourceID, List<SharedSlot>> slotsForGroup = availableSlotsPerJid.get(groupID);

					// sanity check
					if (slotsForGroup == null) {
						throw new IllegalStateException("Trying to return a slot for group " + groupID +
								" when available slots indicated that all slots were available.");
					}

					putIntoMultiMap(slotsForGroup, parent.getTaskManagerID(), parent);
					
				}
				else {
					// this was the last child of the parent. release the parent.
					parent.markCancelled();
					internalDisposeEmptySharedSlot(parent);
				}
			}
		}
		else {
			throw new IllegalStateException(
					"Found a shared slot that is neither a root slot, nor associated with a vertex group.");
		}
	}
	
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	// 将这个 SharedSlot 加入到其它 JobVertex 的可调度资源队列中，也就是说其它的 JobVertex 都可以在这个 SharedSlot 上部署自己的 sub task
	private static void putIntoMultiMap(Map<ResourceID, List<SharedSlot>> map, ResourceID location, SharedSlot slot) {
		map.computeIfAbsent(location, k -> new ArrayList<>()).add(slot);
	}
	
	private static SharedSlot removeFromMultiMap(Map<ResourceID, List<SharedSlot>> map, ResourceID location) {
		List<SharedSlot> slotsForLocation = map.get(location);
		
		if (slotsForLocation == null) {
			return null;
		}
		else {
			// 从末尾获取
			SharedSlot slot = slotsForLocation.remove(slotsForLocation.size() - 1);
			if (slotsForLocation.isEmpty()) {
				map.remove(location);
			}
			
			return slot;
		}
	}
	
	private static SharedSlot pollFromMultiMap(Map<ResourceID, List<SharedSlot>> map) {
		Iterator<Map.Entry<ResourceID, List<SharedSlot>>> iter = map.entrySet().iterator();
		
		while (iter.hasNext()) {
			List<SharedSlot> slots = iter.next().getValue();
			
			if (slots.isEmpty()) {
				iter.remove();
			}
			else if (slots.size() == 1) {
				SharedSlot slot = slots.remove(0);
				iter.remove();
				return slot;
			}
			else {
				return slots.remove(slots.size() - 1);
			}
		}
		
		return null;
	}
	
	private static void removeSlotFromAllEntries(
			Map<AbstractID, Map<ResourceID, List<SharedSlot>>> availableSlots, SharedSlot slot)
	{
		final ResourceID taskManagerId = slot.getTaskManagerID();
		
		for (Map.Entry<AbstractID, Map<ResourceID, List<SharedSlot>>> entry : availableSlots.entrySet()) {
			Map<ResourceID, List<SharedSlot>> map = entry.getValue();

			// 获取TaskManager上的SharedSlot集合
			List<SharedSlot> list = map.get(taskManagerId);
			if (list != null) {
				list.remove(slot);
				if (list.isEmpty()) {
					// 说明该TaskManager上没有可用的shared slot
					map.remove(taskManagerId);
				}
			}
		}
	}
}
