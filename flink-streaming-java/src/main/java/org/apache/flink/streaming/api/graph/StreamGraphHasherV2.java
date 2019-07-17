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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.UdfStreamOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hasher;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.util.StringUtils.byteToHexString;

/**
 * 包含重复的代码，以确保算法不会随着未来的Flink版本而改变.
 * StreamGraphHasher from Flink 1.2. This contains duplicated code to ensure that the algorithm does not change with
 * future Flink versions.
 *
 * <p>DO NOT MODIFY THIS CLASS
 */
public class StreamGraphHasherV2 implements StreamGraphHasher {

	private static final Logger LOG = LoggerFactory.getLogger(StreamGraphHasherV2.class);

	/**
	 * Returns a map with a hash for each {@link StreamNode} of the {@link
	 * StreamGraph}. The hash is used as the {@link JobVertexID} in order to
	 * identify nodes across job submissions if they didn't change.
	 *
	 * <p>The complete {@link StreamGraph} is traversed. The hash is either
	 * computed from the transformation's user-specified id (see
	 * {@link Transformation#getUid()}) or generated in a deterministic way.
	 *
	 * <p>The generated hash is deterministic with respect to:生成的一个node的hash值由以下结构部分组成
	 * <ul>
	 *   <li>node-local properties (node ID),真正计算的时候只考虑了node的id属性，其他的并发度和userfunction可能改变了依然是同一个任务
	 *   <li>chained output nodes, and  能够chain上的output算子
	 *   <li>input nodes hashes input节点的hash值
	 * </ul>
	 *
	 * @return A map from {@code StreamNode#id} to hash as 16-byte array. key为{@code StreamNode#id}，value为16byte数组
	 */
	@Override
	public Map<Integer, byte[]> traverseStreamGraphAndGenerateHashes(StreamGraph streamGraph) {
		// The hash function used to generate the hash
		//hash函数
		final HashFunction hashFunction = Hashing.murmur3_128(0);
		final Map<Integer, byte[]> hashes = new HashMap<>();

		//存储访问过了的节点编号
		Set<Integer> visited = new HashSet<>();
		//入队即将访问的节点对象
		Queue<StreamNode> remaining = new ArrayDeque<>();

		// We need to make the source order deterministic. The source IDs are
		// not returned in the same order, which means that submitting the same
		// program twice might result in different traversal, which breaks the
		// deterministic hash assignment.
		//source是一个流拓扑的起点，从source开始遍历
		//hash值的生成是顺序敏感的（依赖于顺序），因此首先要对source ID集合进行排序
		//因为如果source的ID集合顺序不固定，那意味着多次提交包含该source ID集合的程序时可能导致不同的遍历路径，
		//从而破坏了hash生成的因素
		List<Integer> sources = new ArrayList<>(streamGraph.getSourceIDs());
		Collections.sort(sources);

		//
		// Traverse the graph in a breadth-first manner. Keep in mind that
		// the graph is not a tree and multiple paths to nodes can exist.
		//

		// Start with source nodes
		//按照排好的顺序，进行广度遍历，注意这不是树结构，而是图，因为就一个节点而言，其输入和输出都可能有多条路径
		for (Integer sourceNodeId : sources) {
			remaining.add(streamGraph.getStreamNode(sourceNodeId));
			visited.add(sourceNodeId);
		}

		StreamNode currentNode;
		//从即将访问的节点队列中出队首部的一个元素，没有元素了则结束
		while ((currentNode = remaining.poll()) != null) {
			// Generate the hash code. Because multiple path exist to each
			// node, we might not have all required inputs available to
			// generate the hash code.
			// 给当前节点生成哈希值，并返回是否生成成功
			if (generateNodeHash(currentNode, hashFunction, hashes, streamGraph.isChainingEnabled(), streamGraph)) {
				// Add the child nodes
				//遍历当前节点的所有输出边
				for (StreamEdge outEdge : currentNode.getOutEdges()) {
					//获取输出边的目标顶点（该边另一头的顶点）
					StreamNode child = streamGraph.getTargetVertex(outEdge);

					//如果目标顶点没被访问过，则加入待访问队列和已访问元素集合
					if (!visited.contains(child.getId())) {
						remaining.add(child);
						visited.add(child.getId());
					}
				}
			} else {
				// We will revisit this later.
				// visited中表示的已经访问过的元素，可能在生成hash值的时候是失败的，比如input节点还没有生成就会返回失败等
				// input节点生成完成之后再重新扔进remaing进行hash计算
				visited.remove(currentNode.getId());
			}
		}

		return hashes;
	}

	/**
	 * Generates a hash for the node and returns whether the operation was
	 * successful.
	 *
	 * @param node         The node to generate the hash for
	 * @param hashFunction The hash function to use
	 * @param hashes       The current state of generated hashes
	 * @return <code>true</code> if the node hash has been generated.
	 * <code>false</code>, otherwise. If the operation is not successful, the
	 * hash needs be generated at a later point when all input is available.
	 * @throws IllegalStateException If node has user-specified hash and is
	 *                               intermediate node of a chain
	 */
	private boolean generateNodeHash(
			StreamNode node,
			HashFunction hashFunction,
			Map<Integer, byte[]> hashes,
			boolean isChainingEnabled,
			StreamGraph streamGraph) {

		// Check for user-specified ID
		// 这个就是用户通过setUid设置的节点的id值
		String userSpecifiedHash = node.getTransformationUID();

		if (userSpecifiedHash == null) {
			// Check that all input nodes have their hashes computed
			for (StreamEdge inEdge : node.getInEdges()) {
				// If the input node has not been visited yet, the current
				// node will be visited again at a later point when all input
				// nodes have been visited and their hashes set.
				// 只有当input节点都被生成hash存储之后才能进入后面节点的hash生成
				if (!hashes.containsKey(inEdge.getSourceId())) {
					return false;
				}
			}

			Hasher hasher = hashFunction.newHasher();
			byte[] hash = generateDeterministicHash(node, hasher, hashes, isChainingEnabled, streamGraph);

			if (hashes.put(node.getId(), hash) != null) {
				// Sanity check
				throw new IllegalStateException("Unexpected state. Tried to add node hash " +
						"twice. This is probably a bug in the JobGraph generator.");
			}

			return true;
		} else {
			Hasher hasher = hashFunction.newHasher();
			byte[] hash = generateUserSpecifiedHash(node, hasher);

			// 判断所有的hashes值，必须全部不相同
			for (byte[] previousHash : hashes.values()) {
				if (Arrays.equals(previousHash, hash)) {
					throw new IllegalArgumentException("Hash collision on user-specified ID " +
							"\"" + userSpecifiedHash + "\". " +
							"Most likely cause is a non-unique ID. Please check that all IDs " +
							"specified via `uid(String)` are unique.");
				}
			}

			if (hashes.put(node.getId(), hash) != null) {
				// Sanity check
				throw new IllegalStateException("Unexpected state. Tried to add node hash " +
						"twice. This is probably a bug in the JobGraph generator.");
			}

			return true;
		}
	}

	/**
	 * Generates a hash from a user-specified ID.
	 */
	private byte[] generateUserSpecifiedHash(StreamNode node, Hasher hasher) {
		hasher.putString(node.getTransformationUID(), Charset.forName("UTF-8"));

		// 用户指定了uid之后直接生成指定的hash值就可以了，步骤简化了不少
		return hasher.hash().asBytes();
	}

	/**
	 * Generates a deterministic hash from node-local properties and input and
	 * output edges.
	 */
	private byte[] generateDeterministicHash(
			StreamNode node,
			Hasher hasher,
			Map<Integer, byte[]> hashes,
			boolean isChainingEnabled,
			StreamGraph streamGraph) {

		// Include stream node to hash. We use the current size of the computed
		// hashes as the ID. We cannot use the node's ID, because it is
		// assigned from a static counter. This will result in two identical
		// programs having different hashes.
		generateNodeLocalHash(hasher, hashes.size());

		// Include chained nodes to hash
		// 判断节点是否能够连接起来
		for (StreamEdge outEdge : node.getOutEdges()) {
			if (isChainable(outEdge, isChainingEnabled, streamGraph)) {

				// Use the hash size again, because the nodes are chained to
				// this node. This does not add a hash for the chained nodes.
				// hashsize此时没有变，因为当前节点的还没有加进去，因为这些节点是被融合成了一个，所以使用同一个id值
				generateNodeLocalHash(hasher, hashes.size());
			}
		}

		byte[] hash = hasher.hash().asBytes();

		// Make sure that all input nodes have their hash set before entering
		// this loop (calling this method).
		for (StreamEdge inEdge : node.getInEdges()) {
			byte[] otherHash = hashes.get(inEdge.getSourceId());

			// Sanity check
			if (otherHash == null) {
				throw new IllegalStateException("Missing hash for input node "
						+ streamGraph.getSourceVertex(inEdge) + ". Cannot generate hash for "
						+ node + ".");
			}

			// 这就是每个节点的hash信息都会带上他input节点的hash信息
			for (int j = 0; j < hash.length; j++) {
				hash[j] = (byte) (hash[j] * 37 ^ otherHash[j]);
			}
		}

		if (LOG.isDebugEnabled()) {
			String udfClassName = "";
			if (node.getOperatorFactory() instanceof UdfStreamOperatorFactory) {
					udfClassName = ((UdfStreamOperatorFactory) node.getOperatorFactory()).getUserFunctionClassName();
			}

			LOG.debug("Generated hash '" + byteToHexString(hash) + "' for node " +
					"'" + node.toString() + "' {id: " + node.getId() + ", " +
					"parallelism: " + node.getParallelism() + ", " +
					"user function: " + udfClassName + "}");
		}

		return hash;
	}

	/**
	 * Applies the {@link Hasher} to the {@link StreamNode} . The hasher encapsulates
	 * the current state of the hash.
	 *
	 * <p>The specified ID is local to this node. We cannot use the
	 * {@code StreamNode#id}, because it is incremented in a static counter.
	 * Therefore, the IDs for identical jobs will otherwise be different.
	 */
	private void generateNodeLocalHash(Hasher hasher, int id) {
		// This resolves conflicts for otherwise identical source nodes. BUT
		// the generated hash codes depend on the ordering of the nodes in the
		// stream graph.
		// 没用使用到传入的node参数,查看了下，这个是因为V1中的hash计算和node的属性相关，但V2版本用不到，应该被去除
		hasher.putInt(id);
	}

	private boolean isChainable(StreamEdge edge, boolean isChainingEnabled, StreamGraph streamGraph) {
		StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
		StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

		StreamOperatorFactory<?> headOperator = upStreamVertex.getOperatorFactory();
		StreamOperatorFactory<?> outOperator = downStreamVertex.getOperatorFactory();

		// 判断是否可以chain其实是对Edge进行判断，取其上下游进行判断
		return downStreamVertex.getInEdges().size() == 1
				&& outOperator != null
				&& headOperator != null
				&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
				&& outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
				headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& (edge.getPartitioner() instanceof ForwardPartitioner)
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
				&& isChainingEnabled;
	}
}
