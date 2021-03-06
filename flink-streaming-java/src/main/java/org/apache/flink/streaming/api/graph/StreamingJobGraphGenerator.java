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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.UdfStreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;

/**
 * The StreamingJobGraphGenerator converts a {@link StreamGraph} into a {@link JobGraph}.
 */
@Internal
public class StreamingJobGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

	// ------------------------------------------------------------------------

	public static JobGraph createJobGraph(StreamGraph streamGraph) {
		return createJobGraph(streamGraph, null);
	}

	public static JobGraph createJobGraph(StreamGraph streamGraph, @Nullable JobID jobID) {
		return new StreamingJobGraphGenerator(streamGraph, jobID).createJobGraph();
	}

	// ------------------------------------------------------------------------

	private final StreamGraph streamGraph;

	private final Map<Integer, JobVertex> jobVertices;
	private final JobGraph jobGraph;
	private final Collection<Integer> builtVertices;

	private final List<StreamEdge> physicalEdgesInOrder;

	private final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;

	private final Map<Integer, StreamConfig> vertexConfigs;
	private final Map<Integer, String> chainedNames;

	private final Map<Integer, ResourceSpec> chainedMinResources;
	private final Map<Integer, ResourceSpec> chainedPreferredResources;

	private final Map<Integer, InputOutputFormatContainer> chainedInputOutputFormats;

	private final StreamGraphHasher defaultStreamGraphHasher;
	private final List<StreamGraphHasher> legacyStreamGraphHashers;

	private StreamingJobGraphGenerator(StreamGraph streamGraph) {
		this(streamGraph, null);
	}

	private StreamingJobGraphGenerator(StreamGraph streamGraph, @Nullable JobID jobID) {
		this.streamGraph = streamGraph;
		this.defaultStreamGraphHasher = new StreamGraphHasherV2();
		this.legacyStreamGraphHashers = Arrays.asList(new StreamGraphUserHashHasher());

		this.jobVertices = new HashMap<>();
		this.builtVertices = new HashSet<>();
		this.chainedConfigs = new HashMap<>();
		this.vertexConfigs = new HashMap<>();
		this.chainedNames = new HashMap<>();
		this.chainedMinResources = new HashMap<>();
		this.chainedPreferredResources = new HashMap<>();
		this.chainedInputOutputFormats = new HashMap<>();
		this.physicalEdgesInOrder = new ArrayList<>();

		jobGraph = new JobGraph(jobID, streamGraph.getJobName());
	}

	private JobGraph createJobGraph() {
		preValidate();

		// make sure that all vertices start immediately
		// 设置启动模式为所有节点均在一开始就启动
		jobGraph.setScheduleMode(streamGraph.getScheduleMode());

		// Generate deterministic hashes for the nodes in order to identify them across
		// submission iff they didn't change.
		// 为每个节点生成hash id
		Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);

		// Generate legacy version hashes for backwards compatibility
		// 为了保持兼容性创建的hash
		List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
		for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
			legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
		}

		Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes = new HashMap<>();
		//生成jobvertex，串成chain等
		//这里的逻辑大致可以理解为，挨个遍历节点，如果该节点是一个chain的头节点，就生成一个JobVertex，
		// 如果不是头节点，就要把自身配置并入头节点，然后把头节点和自己的出边相连；
		// 对于不能chain的节点，当作只有头节点处理即可
		setChaining(hashes, legacyHashes, chainedOperatorHashes);

		//设置输入边edge
		setPhysicalEdges();

		//设置slot共享group,两种机制都用于限制算子的部署.其中，CoLocationGroup主要用于迭代算子的执行.
		setSlotSharingAndCoLocation();

		//配置检查点.当用户的Flink程序配置了检查点信息，那么需要将检查点相关的配置加入到JobGraph中去，这部分逻辑通过方法configureCheckpointing来完
		configureCheckpointing();

		JobGraphGenerator.addUserArtifactEntries(streamGraph.getUserArtifacts(), jobGraph);

		// set the ExecutionConfig last when it has been finalized
		// 传递执行环境配置
		try {
			jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
		}
		catch (IOException e) {
			throw new IllegalConfigurationException("Could not serialize the ExecutionConfig." +
					"This indicates that non-serializable types (like custom serializers) were registered");
		}

		return jobGraph;
	}

	@SuppressWarnings("deprecation")
	private void preValidate() {
		CheckpointConfig checkpointConfig = streamGraph.getCheckpointConfig();

		if (checkpointConfig.isCheckpointingEnabled()) {
			// temporarily forbid checkpointing for iterative jobs
			if (streamGraph.isIterative() && !checkpointConfig.isForceCheckpointing()) {
				throw new UnsupportedOperationException(
					"Checkpointing is currently not supported by default for iterative jobs, as we cannot guarantee exactly once semantics. "
						+ "State checkpoints happen normally, but records in-transit during the snapshot will be lost upon failure. "
						+ "\nThe user can force enable state checkpoints with the reduced guarantees by calling: env.enableCheckpointing(interval,true)");
			}

			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			for (StreamNode node : streamGraph.getStreamNodes()) {
				StreamOperatorFactory operatorFactory = node.getOperatorFactory();
				if (operatorFactory != null) {
					Class<?> operatorClass = operatorFactory.getStreamOperatorClass(classLoader);
					if (InputSelectable.class.isAssignableFrom(operatorClass)) {

						throw new UnsupportedOperationException(
							"Checkpointing is currently not supported for operators that implement InputSelectable:"
								+ operatorClass.getName());
					}
				}
			}
		}
	}

	private void setPhysicalEdges() {
		Map<Integer, List<StreamEdge>> physicalInEdgesInOrder = new HashMap<Integer, List<StreamEdge>>();

		for (StreamEdge edge : physicalEdgesInOrder) {
			int target = edge.getTargetId();

			List<StreamEdge> inEdges = physicalInEdgesInOrder.computeIfAbsent(target, k -> new ArrayList<>());

			inEdges.add(edge);
		}

		for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
			int vertex = inEdges.getKey();
			List<StreamEdge> edgeList = inEdges.getValue();

			// 给各个vertex的StreamConfig设置其上游的inEdgeList
			vertexConfigs.get(vertex).setInPhysicalEdges(edgeList);
		}
	}

	/**
	 * Sets up task chains from the source {@link StreamNode} instances.
	 *
	 * <p>This will recursively create all {@link JobVertex} instances.
	 */
	private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes, Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {
		for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
			// 同样是从source处往后创建chain
			createChain(sourceNodeId, sourceNodeId, hashes, legacyHashes, 0, chainedOperatorHashes);
		}
	}

	/**该方法以当前source为起点向后遍历并创建算子链
	 *
	 * @param startNodeId 起始节点编号
	 * @param currentNodeId 当前遍历节点编号
	 * @param hashes  节点编号与hash值映射表
	 * @param legacyHashes 遗留的Hashes映射表
	 * @param chainIndex 当前index
	 * @param chainedOperatorHashes
	 * @return 遍历过的边集合
	 */
	private List<StreamEdge> createChain(
			Integer startNodeId,
			Integer currentNodeId,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			int chainIndex,
			Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {

		// builtVertices表示已经创建过JobVertex节点的streamNode的id
		//如果起始节点没有被构建过，才进入分支；否则直接返回一个空List（递归结束条件）
		if (!builtVertices.contains(startNodeId)) {

			//存储遍历过的边，该对象被作为最终结果返回
			List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();
			//存储可以被链接的出边
			List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
			//存储不可被链接的出边
			List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

			StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);
			//遍历当前节点的每个出边
			for (StreamEdge outEdge : currentNode.getOutEdges()) {
				//如果该出边是可被链接的，则加入可被链接的出边集合，否则加入不可被链接的出边集合
				if (isChainable(outEdge, streamGraph)) {
					chainableOutputs.add(outEdge);
				} else {
					nonChainableOutputs.add(outEdge);
				}
			}

			//遍历每个可被链接的出边，然后进行递归遍历
			for (StreamEdge chainable : chainableOutputs) {
				// 这里是递归的将能够和这个edge chain的edge都串起来并且记录在transitiveOutEdges，他负责收集所有的outEdges，不关chain或者不chain
				//起始节点不变，以该可被链接的出边的目标节点作为“当前”节点进行递归遍历并将遍历过的边集合加入到当前集合中
				//important:这里值得注意的是所有可链接的边本身并不会被加入这个集合！
				transitiveOutEdges.addAll(
						createChain(startNodeId, chainable.getTargetId(), hashes, legacyHashes, chainIndex + 1, chainedOperatorHashes));
			}

			//遍历不可链接的出边，同样进行递归遍历
			for (StreamEdge nonChainable : nonChainableOutputs) {
				//将当前不可链接的出边加入到遍历过的边集合中
				transitiveOutEdges.add(nonChainable);
				//同样进行递归遍历，不过这里的起始节点和当前节点都被设置为该边的目标节点
				createChain(nonChainable.getTargetId(), nonChainable.getTargetId(), hashes, legacyHashes, 0, chainedOperatorHashes);
			}

			// 把startNodeId加入chainedOperatorHashes中，如果存在返回对应的值
			// 这里其实不管是不是可以chain的算子都会放到chainedOperatorHashes  这个应该表示的是做完chain操作之后的算子的hash列表，
			// 里面不包含那些被chain进去的算子的hash因为没有他们的startNodeID，这个首先被放进去的应该是sink处算子的hash，因为是递归调用的
			List<Tuple2<byte[], byte[]>> operatorHashes =
				chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());

			byte[] primaryHashBytes = hashes.get(currentNodeId);
			OperatorID currentOperatorId = new OperatorID(primaryHashBytes);

			for (Map<Integer, byte[]> legacyHash : legacyHashes) {
				operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHash.get(currentNodeId)));
			}

			//为当前节点创建链接的完整名称，如果当前节点没有可链接的边，那么其名称将直接是当前节点的operator名称
			chainedNames.put(currentNodeId, createChainedName(currentNodeId, chainableOutputs));
			// 资源计算主要针对的是chain的算子的计算
			chainedMinResources.put(currentNodeId, createChainedMinResources(currentNodeId, chainableOutputs));
			chainedPreferredResources.put(currentNodeId, createChainedPreferredResources(currentNodeId, chainableOutputs));

			if (currentNode.getInputFormat() != null) {
				getOrCreateFormatContainer(startNodeId).addInputFormat(currentOperatorId, currentNode.getInputFormat());
			}

			if (currentNode.getOutputFormat() != null) {
				getOrCreateFormatContainer(startNodeId).addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
			}

			//创建流配置对象，流配置对象针对单个作业顶点而言，包含了顶点相关的所有信息.
			//当创建配置对象的时候，如果当前节点即为起始节点（链接头），会先为该节点创建JobVertex对象
			StreamConfig config = currentNodeId.equals(startNodeId)
					? createJobVertex(startNodeId, hashes, legacyHashes, chainedOperatorHashes)
					: new StreamConfig(new Configuration());

			//然后为当前节点初始化流配置对象里的一系列属性
			setVertexConfig(currentNodeId, config, chainableOutputs, nonChainableOutputs);

			//如果当前节点是起始节点（chain头节点）
			if (currentNodeId.equals(startNodeId)) {

				//设置该节点是chain的开始
				config.setChainStart();
				config.setChainIndex(0);
				config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());
				//设置不可链接的出边
				config.setOutEdgesInOrder(transitiveOutEdges);
				//设置所有出边
				config.setOutEdges(streamGraph.getStreamNode(currentNodeId).getOutEdges());

				//遍历当前节点的所有不可链接的出边集合
				for (StreamEdge edge : transitiveOutEdges) {
					//给当前节点到不可链接的出边之间建立连接
					//通过出边找到其下游流节点，根据边的分区器类型，构建下游流节点跟输入端上游流节点（也即起始节点）
					//的连接关系.在这个构建的过程中也就创建了IntermediateDataSet及JobEdge并跟当前节点的JobVertex
					//三者建立了关联关系
					connect(startNodeId, edge);
				}

				//将当前节点的所有子节点的流配置对象进行序列化
				config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));

			} else { //如果当前节点是chain中的节点，而非chain的头节点
				chainedConfigs.computeIfAbsent(startNodeId, k -> new HashMap<Integer, StreamConfig>());

				config.setChainIndex(chainIndex);
				StreamNode node = streamGraph.getStreamNode(currentNodeId);
				config.setOperatorName(node.getOperatorName());
				//将当前节点的流配置对象加入到chain头节点点相关的配置中
				chainedConfigs.get(startNodeId).put(currentNodeId, config);
			}

			config.setOperatorID(currentOperatorId);

			// 可链接的出边为空，则链接结束
			if (chainableOutputs.isEmpty()) {
				config.setChainEnd();
			}
			//返回所有不可链接的边
			return transitiveOutEdges;

		} else {
			return new ArrayList<>();
		}
	}

	private InputOutputFormatContainer getOrCreateFormatContainer(Integer startNodeId) {
		return chainedInputOutputFormats
			.computeIfAbsent(startNodeId, k -> new InputOutputFormatContainer(Thread.currentThread().getContextClassLoader()));
	}

	private String createChainedName(Integer vertexID, List<StreamEdge> chainedOutputs) {
		String operatorName = streamGraph.getStreamNode(vertexID).getOperatorName();
		if (chainedOutputs.size() > 1) {
			List<String> outputChainedNames = new ArrayList<>();
			for (StreamEdge chainable : chainedOutputs) {
				outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
			}
			return operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
		} else if (chainedOutputs.size() == 1) {
			return operatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
		} else {
			// 没有chain的话就返回自己的名字
			return operatorName;
		}
	}

	private ResourceSpec createChainedMinResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
		ResourceSpec minResources = streamGraph.getStreamNode(vertexID).getMinResources();
		// chain的资源是将各个资源相加起来  每个节点有资源的概念
		for (StreamEdge chainable : chainedOutputs) {
			minResources = minResources.merge(chainedMinResources.get(chainable.getTargetId()));
		}
		return minResources;
	}

	private ResourceSpec createChainedPreferredResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
		ResourceSpec preferredResources = streamGraph.getStreamNode(vertexID).getPreferredResources();
		for (StreamEdge chainable : chainedOutputs) {
			preferredResources = preferredResources.merge(chainedPreferredResources.get(chainable.getTargetId()));
		}
		return preferredResources;
	}

	private StreamConfig createJobVertex(
			Integer streamNodeId,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {

		JobVertex jobVertex;
		StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);

		byte[] hash = hashes.get(streamNodeId);

		if (hash == null) {
			throw new IllegalStateException("Cannot find node hash. " +
					"Did you generate them before calling this method?");
		}

		JobVertexID jobVertexId = new JobVertexID(hash);

		List<JobVertexID> legacyJobVertexIds = new ArrayList<>(legacyHashes.size());
		for (Map<Integer, byte[]> legacyHash : legacyHashes) {
			hash = legacyHash.get(streamNodeId);
			if (null != hash) {
				legacyJobVertexIds.add(new JobVertexID(hash));
			}
		}

		List<Tuple2<byte[], byte[]>> chainedOperators = chainedOperatorHashes.get(streamNodeId);
		List<OperatorID> chainedOperatorVertexIds = new ArrayList<>();
		List<OperatorID> userDefinedChainedOperatorVertexIds = new ArrayList<>();
		if (chainedOperators != null) {
			for (Tuple2<byte[], byte[]> chainedOperator : chainedOperators) {
				chainedOperatorVertexIds.add(new OperatorID(chainedOperator.f0));
				userDefinedChainedOperatorVertexIds.add(chainedOperator.f1 != null ? new OperatorID(chainedOperator.f1) : null);
			}
		}

		if (chainedInputOutputFormats.containsKey(streamNodeId)) {
			jobVertex = new InputOutputFormatVertex(
					chainedNames.get(streamNodeId),
					jobVertexId,
					legacyJobVertexIds,
					chainedOperatorVertexIds,
					userDefinedChainedOperatorVertexIds);

			chainedInputOutputFormats
				.get(streamNodeId)
				.write(new TaskConfig(jobVertex.getConfiguration()));
		} else {
			jobVertex = new JobVertex(
					chainedNames.get(streamNodeId),
					jobVertexId,
					legacyJobVertexIds,
					chainedOperatorVertexIds,
					userDefinedChainedOperatorVertexIds);
		}

		jobVertex.setResources(chainedMinResources.get(streamNodeId), chainedPreferredResources.get(streamNodeId));

		jobVertex.setInvokableClass(streamNode.getJobVertexClass());

		int parallelism = streamNode.getParallelism();

		if (parallelism > 0) {
			jobVertex.setParallelism(parallelism);
		} else {
			parallelism = jobVertex.getParallelism();
		}

		jobVertex.setMaxParallelism(streamNode.getMaxParallelism());

		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
		}

		// TODO: inherit InputDependencyConstraint from the head operator
		jobVertex.setInputDependencyConstraint(streamGraph.getExecutionConfig().getDefaultInputDependencyConstraint());

		jobVertices.put(streamNodeId, jobVertex);
		builtVertices.add(streamNodeId);
		jobGraph.addVertex(jobVertex);

		return new StreamConfig(jobVertex.getConfiguration());
	}

	@SuppressWarnings("unchecked")
	private void setVertexConfig(Integer vertexID, StreamConfig config,
			List<StreamEdge> chainableOutputs, List<StreamEdge> nonChainableOutputs) {

		StreamNode vertex = streamGraph.getStreamNode(vertexID);

		config.setVertexID(vertexID);
		config.setBufferTimeout(vertex.getBufferTimeout());

		config.setTypeSerializerIn1(vertex.getTypeSerializerIn1());
		config.setTypeSerializerIn2(vertex.getTypeSerializerIn2());
		config.setTypeSerializerOut(vertex.getTypeSerializerOut());

		// iterate edges, find sideOutput edges create and save serializers for each outputTag type
		for (StreamEdge edge : chainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
					edge.getOutputTag(),
					edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}
		for (StreamEdge edge : nonChainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
						edge.getOutputTag(),
						edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}

		config.setStreamOperatorFactory(vertex.getOperatorFactory());
		config.setOutputSelectors(vertex.getOutputSelectors());

		config.setNumberOfOutputs(nonChainableOutputs.size());
		config.setNonChainedOutputs(nonChainableOutputs);
		config.setChainedOutputs(chainableOutputs);

		config.setTimeCharacteristic(streamGraph.getTimeCharacteristic());

		final CheckpointConfig checkpointCfg = streamGraph.getCheckpointConfig();

		config.setStateBackend(streamGraph.getStateBackend());
		config.setCheckpointingEnabled(checkpointCfg.isCheckpointingEnabled());
		if (checkpointCfg.isCheckpointingEnabled()) {
			config.setCheckpointMode(checkpointCfg.getCheckpointingMode());
		}
		else {
			// the "at-least-once" input handler is slightly cheaper (in the absence of checkpoints),
			// so we use that one if checkpointing is not enabled
			config.setCheckpointMode(CheckpointingMode.AT_LEAST_ONCE);
		}
		config.setStatePartitioner(0, vertex.getStatePartitioner1());
		config.setStatePartitioner(1, vertex.getStatePartitioner2());
		config.setStateKeySerializer(vertex.getStateKeySerializer());

		Class<? extends AbstractInvokable> vertexClass = vertex.getJobVertexClass();

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(streamGraph.getBrokerID(vertexID));
			config.setIterationWaitTime(streamGraph.getLoopTimeout(vertexID));
		}

		vertexConfigs.put(vertexID, config);
	}

	private void connect(Integer headOfChain, StreamEdge edge) {

		physicalEdgesInOrder.add(edge);

		Integer downStreamvertexID = edge.getTargetId();

		JobVertex headVertex = jobVertices.get(headOfChain);
		JobVertex downStreamVertex = jobVertices.get(downStreamvertexID);

		StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

		downStreamConfig.setNumberOfInputs(downStreamConfig.getNumberOfInputs() + 1);

		// 根据partitioner定义上下游的连接规则，其实是生成相应的JobEdge
		StreamPartitioner<?> partitioner = edge.getPartitioner();

		ResultPartitionType resultPartitionType;
		switch (edge.getShuffleMode()) {
			case PIPELINED:
				resultPartitionType = ResultPartitionType.PIPELINED_BOUNDED;
				break;
			case BATCH:
				resultPartitionType = ResultPartitionType.BLOCKING;
				break;
			case UNDEFINED:
				resultPartitionType = streamGraph.isBlockingConnectionsBetweenChains() ?
						ResultPartitionType.BLOCKING : ResultPartitionType.PIPELINED_BOUNDED;
				break;
			default:
				throw new UnsupportedOperationException("Data exchange mode " +
					edge.getShuffleMode() + " is not supported yet.");
		}

		JobEdge jobEdge;
		if (partitioner instanceof ForwardPartitioner || partitioner instanceof RescalePartitioner) {
			// 这里涉及到了IntermediateDataSet的生成，具体代码见JobVertex内部
			jobEdge = downStreamVertex.connectNewDataSetAsInput(
				headVertex,
				DistributionPattern.POINTWISE,
				resultPartitionType);
		} else {
			jobEdge = downStreamVertex.connectNewDataSetAsInput(
					headVertex,
					DistributionPattern.ALL_TO_ALL,
					resultPartitionType);
		}
		// set strategy name so that web interface can show it.
		jobEdge.setShipStrategyName(partitioner.toString());

		if (LOG.isDebugEnabled()) {
			LOG.debug("CONNECTED: {} - {} -> {}", partitioner.getClass().getSimpleName(),
					headOfChain, downStreamvertexID);
		}
	}

	public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
		StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
		StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

		StreamOperatorFactory<?> headOperator = upStreamVertex.getOperatorFactory();
		StreamOperatorFactory<?> outOperator = downStreamVertex.getOperatorFactory();

		return downStreamVertex.getInEdges().size() == 1
				&& outOperator != null
				&& headOperator != null
				&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
				&& outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
					headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& (edge.getPartitioner() instanceof ForwardPartitioner)
				&& edge.getShuffleMode() != ShuffleMode.BATCH
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
				&& streamGraph.isChainingEnabled();
	}

	private void setSlotSharingAndCoLocation() {
		final HashMap<String, SlotSharingGroup> slotSharingGroups = new HashMap<>();
		final HashMap<String, Tuple2<SlotSharingGroup, CoLocationGroup>> coLocationGroups = new HashMap<>();

		for (Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

			final StreamNode node = streamGraph.getStreamNode(entry.getKey());
			final JobVertex vertex = entry.getValue();

			// configure slot sharing group
			final String slotSharingGroupKey = node.getSlotSharingGroup();
			final SlotSharingGroup sharingGroup;

			if (slotSharingGroupKey != null) {
				sharingGroup = slotSharingGroups.computeIfAbsent(
						slotSharingGroupKey, (k) -> new SlotSharingGroup());
				vertex.setSlotSharingGroup(sharingGroup);
			} else {
				sharingGroup = null;
			}

			// configure co-location constraint
			final String coLocationGroupKey = node.getCoLocationGroup();
			if (coLocationGroupKey != null) {
				if (sharingGroup == null) {
					throw new IllegalStateException("Cannot use a co-location constraint without a slot sharing group");
				}

				Tuple2<SlotSharingGroup, CoLocationGroup> constraint = coLocationGroups.computeIfAbsent(
						coLocationGroupKey, (k) -> new Tuple2<>(sharingGroup, new CoLocationGroup()));

				if (constraint.f0 != sharingGroup) {
					throw new IllegalStateException("Cannot co-locate operators from different slot sharing groups");
				}

				vertex.updateCoLocationGroup(constraint.f1);
				constraint.f1.addVertex(vertex);
			}
		}
	}

	private void configureCheckpointing() {
		CheckpointConfig cfg = streamGraph.getCheckpointConfig();

		long interval = cfg.getCheckpointInterval();
		if (interval < MINIMAL_CHECKPOINT_TIME) {
			// interval of max value means disable periodic checkpoint
			interval = Long.MAX_VALUE;
		}

		//  --- configure the participating vertices ---

		// collect the vertices that receive "trigger checkpoint" messages.
		// currently, these are all the sources
		// 存储接收“触发检查点”消息的JobVertex集合，当前只收集source顶点
		List<JobVertexID> triggerVertices = new ArrayList<>();

		// collect the vertices that need to acknowledge the checkpoint
		// currently, these are all vertices
		// 收集需要应答检查点消息的JobVertex集合，当前收集所有的JobVertex
		List<JobVertexID> ackVertices = new ArrayList<>(jobVertices.size());

		// collect the vertices that receive "commit checkpoint" messages
		// currently, these are all vertices
		// 存储接收“提交检查点”消息的JobVertex集合，当前收集所有JobVertex
		List<JobVertexID> commitVertices = new ArrayList<>(jobVertices.size());

		for (JobVertex vertex : jobVertices.values()) {
			if (vertex.isInputVertex()) {
				triggerVertices.add(vertex.getID());
			}
			commitVertices.add(vertex.getID());
			ackVertices.add(vertex.getID());
		}

		//  --- configure options ---

		CheckpointRetentionPolicy retentionAfterTermination;
		if (cfg.isExternalizedCheckpointsEnabled()) {
			CheckpointConfig.ExternalizedCheckpointCleanup cleanup = cfg.getExternalizedCheckpointCleanup();
			// Sanity check
			if (cleanup == null) {
				throw new IllegalStateException("Externalized checkpoints enabled, but no cleanup mode configured.");
			}
			retentionAfterTermination = cleanup.deleteOnCancellation() ?
					CheckpointRetentionPolicy.RETAIN_ON_FAILURE :
					CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
		} else {
			retentionAfterTermination = CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
		}

		CheckpointingMode mode = cfg.getCheckpointingMode();

		boolean isExactlyOnce;
		if (mode == CheckpointingMode.EXACTLY_ONCE) {
			isExactlyOnce = true;
		} else if (mode == CheckpointingMode.AT_LEAST_ONCE) {
			isExactlyOnce = false;
		} else {
			throw new IllegalStateException("Unexpected checkpointing mode. " +
				"Did not expect there to be another checkpointing mode besides " +
				"exactly-once or at-least-once.");
		}

		//  --- configure the master-side checkpoint hooks ---

		final ArrayList<MasterTriggerRestoreHook.Factory> hooks = new ArrayList<>();

		for (StreamNode node : streamGraph.getStreamNodes()) {
			if (node.getOperatorFactory() instanceof UdfStreamOperatorFactory) {
				Function f = ((UdfStreamOperatorFactory) node.getOperatorFactory()).getUserFunction();

				// 对于实现了这个接口的userFunction，记录他的hooks，这个hook在做checkpoint的时候在master节点触发操作
				if (f instanceof WithMasterCheckpointHook) {
					hooks.add(new FunctionMasterCheckpointHookFactory((WithMasterCheckpointHook<?>) f));
				}
			}
		}

		// because the hooks can have user-defined code, they need to be stored as
		// eagerly serialized values
		final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks;
		if (hooks.isEmpty()) {
			serializedHooks = null;
		} else {
			try {
				MasterTriggerRestoreHook.Factory[] asArray =
						hooks.toArray(new MasterTriggerRestoreHook.Factory[hooks.size()]);
				serializedHooks = new SerializedValue<>(asArray);
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("Trigger/restore hook is not serializable", e);
			}
		}

		// because the state backend can have user-defined code, it needs to be stored as
		// eagerly serialized value
		final SerializedValue<StateBackend> serializedStateBackend;
		if (streamGraph.getStateBackend() == null) {
			serializedStateBackend = null;
		} else {
			try {
				serializedStateBackend =
					new SerializedValue<StateBackend>(streamGraph.getStateBackend());
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("State backend is not serializable", e);
			}
		}

		//  --- done, put it all together ---

		// 这些信息都被封装在JobSnapshottingSettings对象中，然后被存储到JobGraph
		JobCheckpointingSettings settings = new JobCheckpointingSettings(
			triggerVertices,
			ackVertices,
			commitVertices,
			new CheckpointCoordinatorConfiguration(
				interval,
				cfg.getCheckpointTimeout(),
				cfg.getMinPauseBetweenCheckpoints(),
				cfg.getMaxConcurrentCheckpoints(),
				retentionAfterTermination,
				isExactlyOnce,
				cfg.isPreferCheckpointForRecovery(),
				cfg.getTolerableCheckpointFailureNumber()),
			serializedStateBackend,
			serializedHooks);

		jobGraph.setSnapshotSettings(settings);
	}
}
