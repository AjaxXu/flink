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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.InputFormatOperatorFactory;
import org.apache.flink.streaming.api.operators.OutputFormatOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.CoFeedbackTransformation;
import org.apache.flink.streaming.api.transformations.FeedbackTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.SelectTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.api.transformations.SplitTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.UnionTransformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A generator that generates a {@link StreamGraph} from a graph of
 * {@link Transformation}s.
 *
 * <p>This traverses the tree of {@code Transformations} starting from the sinks. At each
 * transformation we recursively transform the inputs, then create a node in the {@code StreamGraph}
 * and add edges from the input Nodes to our newly created node. The transformation methods
 * return the IDs of the nodes in the StreamGraph that represent the input transformation. Several
 * IDs can be returned to be able to deal with feedback transformations and unions.
 *
 * <p>Partitioning, split/select and union don't create actual nodes in the {@code StreamGraph}. For
 * these, we create a virtual node in the {@code StreamGraph} that holds the specific property, i.e.
 * partitioning, selector and so on. When an edge is created from a virtual node to a downstream
 * node the {@code StreamGraph} resolved the id of the original node and creates an edge
 * in the graph with the desired property. For example, if you have this graph:
 *
 * <pre>
 *     Map-1 -&gt; HashPartition-2 -&gt; Map-3
 * </pre>
 *
 * <p>where the numbers represent transformation IDs. We first recurse all the way down. {@code Map-1}
 * is transformed, i.e. we create a {@code StreamNode} with ID 1. Then we transform the
 * {@code HashPartition}, for this, we create virtual node of ID 4 that holds the property
 * {@code HashPartition}. This transformation returns the ID 4. Then we transform the {@code Map-3}.
 * We add the edge {@code 4 -> 3}. The {@code StreamGraph} resolved the actual node with ID 1 and
 * creates and edge {@code 1 -> 3} with the property HashPartition.
 */
@Internal
public class StreamGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamGraphGenerator.class);

	public static final int DEFAULT_LOWER_BOUND_MAX_PARALLELISM = KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

	public static final ScheduleMode DEFAULT_SCHEDULE_MODE = ScheduleMode.EAGER;

	public static final TimeCharacteristic DEFAULT_TIME_CHARACTERISTIC = TimeCharacteristic.ProcessingTime;

	public static final String DEFAULT_JOB_NAME = "Flink Streaming Job";

	/** The default buffer timeout (max delay of records in the network stack). */
	public static final long DEFAULT_NETWORK_BUFFER_TIMEOUT = 100L;

	public static final String DEFAULT_SLOT_SHARING_GROUP = "default";

	private final List<Transformation<?>> transformations;

	private final ExecutionConfig executionConfig;

	private final CheckpointConfig checkpointConfig;

	private StateBackend stateBackend;

	private boolean chaining = true;

	private boolean isSlotSharingEnabled = true;

	private ScheduleMode scheduleMode = DEFAULT_SCHEDULE_MODE;

	private Collection<Tuple2<String, DistributedCache.DistributedCacheEntry>> userArtifacts;

	private TimeCharacteristic timeCharacteristic = DEFAULT_TIME_CHARACTERISTIC;

	private long defaultBufferTimeout = DEFAULT_NETWORK_BUFFER_TIMEOUT;

	private String jobName = DEFAULT_JOB_NAME;

	/**
	 * If there are some stream edges that can not be chained and the shuffle mode of edge is not
	 * specified, translate these edges into {@code BLOCKING} result partition type.
	 */
	private boolean blockingConnectionsBetweenChains = false;

	// This is used to assign a unique ID to iteration source/sink
	protected static Integer iterationIdCounter = 0;
	public static int getNewIterationNodeId() {
		iterationIdCounter--;
		return iterationIdCounter;
	}

	private StreamGraph streamGraph;

	// Keep track of which Transforms we have already transformed, this is necessary because
	// we have loops, i.e. feedback edges.
	private Map<Transformation<?>, Collection<Integer>> alreadyTransformed;

	public StreamGraphGenerator(List<Transformation<?>> transformations, ExecutionConfig executionConfig, CheckpointConfig checkpointConfig) {
		this.transformations = checkNotNull(transformations);
		this.executionConfig = checkNotNull(executionConfig);
		this.checkpointConfig = checkNotNull(checkpointConfig);
	}

	public StreamGraphGenerator setStateBackend(StateBackend stateBackend) {
		this.stateBackend = stateBackend;
		return this;
	}

	public StreamGraphGenerator setChaining(boolean chaining) {
		this.chaining = chaining;
		return this;
	}

	public StreamGraphGenerator setSlotSharingEnabled(boolean isSlotSharingEnabled) {
		this.isSlotSharingEnabled = isSlotSharingEnabled;
		return this;
	}

	public StreamGraphGenerator setScheduleMode(ScheduleMode scheduleMode) {
		this.scheduleMode = scheduleMode;
		return this;
	}

	public StreamGraphGenerator setUserArtifacts(Collection<Tuple2<String, DistributedCache.DistributedCacheEntry>> userArtifacts) {
		this.userArtifacts = userArtifacts;
		return this;
	}

	public StreamGraphGenerator setTimeCharacteristic(TimeCharacteristic timeCharacteristic) {
		this.timeCharacteristic = timeCharacteristic;
		return this;
	}

	public StreamGraphGenerator setDefaultBufferTimeout(long defaultBufferTimeout) {
		this.defaultBufferTimeout = defaultBufferTimeout;
		return this;
	}

	public StreamGraphGenerator setJobName(String jobName) {
		this.jobName = jobName;
		return this;
	}

	public StreamGraphGenerator setBlockingConnectionsBetweenChains(boolean blockingConnectionsBetweenChains) {
		this.blockingConnectionsBetweenChains = blockingConnectionsBetweenChains;
		return this;
	}

	public StreamGraph generate() {
		streamGraph = new StreamGraph(executionConfig, checkpointConfig);
		streamGraph.setStateBackend(stateBackend);
		streamGraph.setChaining(chaining);
		streamGraph.setScheduleMode(scheduleMode);
		streamGraph.setUserArtifacts(userArtifacts);
		streamGraph.setTimeCharacteristic(timeCharacteristic);
		streamGraph.setJobName(jobName);
		streamGraph.setBlockingConnectionsBetweenChains(blockingConnectionsBetweenChains);

		alreadyTransformed = new HashMap<>();

		for (Transformation<?> transformation: transformations) {
			transform(transformation);
		}

		final StreamGraph builtStreamGraph = streamGraph;

		alreadyTransformed.clear();
		alreadyTransformed = null;
		streamGraph = null;

		return builtStreamGraph;
	}

	/**
<<<<<<< HEAD
	 * 在transform方法中，它枚举了Flink中每一种转换类型，并对当前传入的转换类型进行判断，然后将其分发给特定的转换方法进行转换，
	 * 最终返回当前StreamGraph对象中跟该转换有关的节点编号集合
	 * Transforms one {@code StreamTransformation}.
=======
	 * Transforms one {@code Transformation}.
>>>>>>> apache-master
	 *
	 * <p>This checks whether we already transformed it and exits early in that case. If not it
	 * delegates to one of the transformation specific methods.
	 *
	 * 这个方法的核心逻辑就是判断传入的StreamTransformation是哪种类型，并执行相应的操作，详情见下面那一大堆if-else
	 */
	private Collection<Integer> transform(Transformation<?> transform) {

		if (alreadyTransformed.containsKey(transform)) {
			return alreadyTransformed.get(transform);
		}

		LOG.debug("Transforming " + transform);

		if (transform.getMaxParallelism() <= 0) {

			// if the max parallelism hasn't been set, then first use the job wide max parallelism
			// from the ExecutionConfig.
			int globalMaxParallelismFromConfig = executionConfig.getMaxParallelism();
			if (globalMaxParallelismFromConfig > 0) {
				transform.setMaxParallelism(globalMaxParallelismFromConfig);
			}
		}

		// call at least once to trigger exceptions about MissingTypeInfo
		transform.getOutputType();

		Collection<Integer> transformedIds;
		//这里对操作符的类型进行判断，并以此调用相应的处理逻辑.简而言之，处理的核心无非是递归的将该节点和节点的上游节点加入图
		if (transform instanceof OneInputTransformation<?, ?>) {
			transformedIds = transformOneInputTransform((OneInputTransformation<?, ?>) transform);
		} else if (transform instanceof TwoInputTransformation<?, ?, ?>) {
			transformedIds = transformTwoInputTransform((TwoInputTransformation<?, ?, ?>) transform);
		} else if (transform instanceof SourceTransformation<?>) {
			transformedIds = transformSource((SourceTransformation<?>) transform);
		} else if (transform instanceof SinkTransformation<?>) {
			transformedIds = transformSink((SinkTransformation<?>) transform);
		} else if (transform instanceof UnionTransformation<?>) {
			transformedIds = transformUnion((UnionTransformation<?>) transform);
		} else if (transform instanceof SplitTransformation<?>) {
			transformedIds = transformSplit((SplitTransformation<?>) transform);
		} else if (transform instanceof SelectTransformation<?>) {
			transformedIds = transformSelect((SelectTransformation<?>) transform);
		} else if (transform instanceof FeedbackTransformation<?>) {
			transformedIds = transformFeedback((FeedbackTransformation<?>) transform);
		} else if (transform instanceof CoFeedbackTransformation<?>) {
			transformedIds = transformCoFeedback((CoFeedbackTransformation<?>) transform);
		} else if (transform instanceof PartitionTransformation<?>) {
			transformedIds = transformPartition((PartitionTransformation<?>) transform);
		} else if (transform instanceof SideOutputTransformation<?>) {
			transformedIds = transformSideOutput((SideOutputTransformation<?>) transform);
		} else {
			throw new IllegalStateException("Unknown transformation: " + transform);
		}

		//注意这里和函数开始时的方法相对应，在有向图中要注意避免循环的产生
		// need this check because the iterate transformation adds itself before
		// transforming the feedback edges
		if (!alreadyTransformed.containsKey(transform)) {
			alreadyTransformed.put(transform, transformedIds);
		}

		if (transform.getBufferTimeout() >= 0) {
			streamGraph.setBufferTimeout(transform.getId(), transform.getBufferTimeout());
		} else {
			streamGraph.setBufferTimeout(transform.getId(), defaultBufferTimeout);
		}

		if (transform.getUid() != null) {
			streamGraph.setTransformationUID(transform.getId(), transform.getUid());
		}
		if (transform.getUserProvidedNodeHash() != null) {
			streamGraph.setTransformationUserHash(transform.getId(), transform.getUserProvidedNodeHash());
		}

		if (!streamGraph.getExecutionConfig().hasAutoGeneratedUIDsEnabled()) {
			if (transform.getUserProvidedNodeHash() == null && transform.getUid() == null) {
				throw new IllegalStateException("Auto generated UIDs have been disabled " +
					"but no UID or hash has been assigned to operator " + transform.getName());
			}
		}

		if (transform.getMinResources() != null && transform.getPreferredResources() != null) {
			streamGraph.setResources(transform.getId(), transform.getMinResources(), transform.getPreferredResources());
		}

		return transformedIds;
	}

	/**
	 * Transforms a {@code UnionTransformation}.
	 *
	 * <p>This is easy, we only have to transform the inputs and return all the IDs in a list so
	 * that downstream operations can connect to all upstream nodes.
	 */
	private <T> Collection<Integer> transformUnion(UnionTransformation<T> union) {
		List<Transformation<T>> inputs = union.getInputs();
		List<Integer> resultIds = new ArrayList<>();

		for (Transformation<T> input: inputs) {
			resultIds.addAll(transform(input));
		}

		return resultIds;
	}

	/**
	 * Transforms a {@code PartitionTransformation}.
	 *
	 * <p>For this we create a virtual node in the {@code StreamGraph} that holds the partition
	 * property. @see StreamGraphGenerator
	 */
	private <T> Collection<Integer> transformPartition(PartitionTransformation<T> partition) {
		Transformation<T> input = partition.getInput();
		List<Integer> resultIds = new ArrayList<>();

		Collection<Integer> transformedIds = transform(input);
		for (Integer transformedId: transformedIds) {
			int virtualId = Transformation.getNewNodeId();
			streamGraph.addVirtualPartitionNode(
					transformedId, virtualId, partition.getPartitioner(), partition.getShuffleMode());
			resultIds.add(virtualId);
		}

		return resultIds;
	}

	/**
	 * Transforms a {@code SplitTransformation}.
	 *
	 * <p>We add the output selector to previously transformed nodes.
	 */
	private <T> Collection<Integer> transformSplit(SplitTransformation<T> split) {

		Transformation<T> input = split.getInput();
		Collection<Integer> resultIds = transform(input);

		validateSplitTransformation(input);

		// the recursive transform call might have transformed this already
		if (alreadyTransformed.containsKey(split)) {
			return alreadyTransformed.get(split);
		}

		for (int inputId : resultIds) {
			streamGraph.addOutputSelector(inputId, split.getOutputSelector());
		}

		return resultIds;
	}

	/**
	 * Transforms a {@code SelectTransformation}.
	 *
	 * <p>For this we create a virtual node in the {@code StreamGraph} holds the selected names.
	 *
	 * @see org.apache.flink.streaming.api.graph.StreamGraphGenerator
	 */
	private <T> Collection<Integer> transformSelect(SelectTransformation<T> select) {
		Transformation<T> input = select.getInput();
		Collection<Integer> resultIds = transform(input);

		// the recursive transform might have already transformed this
		if (alreadyTransformed.containsKey(select)) {
			return alreadyTransformed.get(select);
		}

		List<Integer> virtualResultIds = new ArrayList<>();

		for (int inputId : resultIds) {
			int virtualId = Transformation.getNewNodeId();
			streamGraph.addVirtualSelectNode(inputId, virtualId, select.getSelectedNames());
			virtualResultIds.add(virtualId);
		}
		return virtualResultIds;
	}

	/**
	 * Transforms a {@code SideOutputTransformation}.
	 *
	 * <p>For this we create a virtual node in the {@code StreamGraph} that holds the side-output
	 * {@link org.apache.flink.util.OutputTag}.
	 *
	 * @see org.apache.flink.streaming.api.graph.StreamGraphGenerator
	 */
	private <T> Collection<Integer> transformSideOutput(SideOutputTransformation<T> sideOutput) {
		Transformation<?> input = sideOutput.getInput();
		Collection<Integer> resultIds = transform(input);

		// the recursive transform might have already transformed this
		if (alreadyTransformed.containsKey(sideOutput)) {
			return alreadyTransformed.get(sideOutput);
		}

		List<Integer> virtualResultIds = new ArrayList<>();

		for (int inputId : resultIds) {
			int virtualId = Transformation.getNewNodeId();
			streamGraph.addVirtualSideOutputNode(inputId, virtualId, sideOutput.getOutputTag());
			virtualResultIds.add(virtualId);
		}
		return virtualResultIds;
	}

	/**
	 * Transforms a {@code FeedbackTransformation}.
	 *
	 * <p>This will recursively transform the input and the feedback edges. We return the
	 * concatenation of the input IDs and the feedback IDs so that downstream operations can be
	 * wired to both.
	 *
	 * <p>This is responsible for creating the IterationSource and IterationSink which are used to
	 * feed back the elements.
	 */
	private <T> Collection<Integer> transformFeedback(FeedbackTransformation<T> iterate) {

		//检查迭代的反馈边，如果没有反馈边，则无法形成迭代的“环”，这时就抛出异常
		if (iterate.getFeedbackEdges().size() <= 0) {
			throw new IllegalStateException("Iteration " + iterate + " does not have any feedback edges.");
		}

		//获得迭代的上游输入端对应的转换
		Transformation<T> input = iterate.getInput();
		List<Integer> resultIds = new ArrayList<>();

		// first transform the input stream(s) and store the result IDs
		//对上游输入进行（递归）转换以获得转换编号集合
		Collection<Integer> inputIds = transform(input);
		//将转换编号集合加入结果集合中
		resultIds.addAll(inputIds);

		// the recursive transform might have already transformed this
		//因为转换是递归进行的，所以为防止重复转换，会将已转换过的对象将入alreadyTransformed集合中
		//在对当前转换对象进行转换之前会预先检查该集合，如果当前转换对象已处于该集合中，则直接返回对应的编号集合，防止重复转换
		if (alreadyTransformed.containsKey(iterate)) {
			return alreadyTransformed.get(iterate);
		}

		// create the fake iteration source/sink pair
		//这里将迭代这一在执行图中的环看作一个闭合的整体，认为它也有source和sink，为其创建source和sink的二元组
		Tuple2<StreamNode, StreamNode> itSourceAndSink = streamGraph.createIterationSourceAndSink(
			iterate.getId(),
			getNewIterationNodeId(),
			getNewIterationNodeId(),
			iterate.getWaitTime(),
			iterate.getParallelism(),
			iterate.getMaxParallelism(),
			iterate.getMinResources(),
			iterate.getPreferredResources());

		//获得迭代source和sink
		StreamNode itSource = itSourceAndSink.f0;
		StreamNode itSink = itSourceAndSink.f1;

		// We set the proper serializers for the sink/source
		//在StreamGraph中为这两个顶点设置序列化器
		streamGraph.setSerializers(itSource.getId(), null, null, iterate.getOutputType().createSerializer(executionConfig));
		streamGraph.setSerializers(itSink.getId(), iterate.getOutputType().createSerializer(executionConfig), null, null);

		// also add the feedback source ID to the result IDs, so that downstream operators will
		// add both as input
		//将迭代的source顶点的编号也作为结果集合的一部分，这是为了让下游的算子将其视为输入
		resultIds.add(itSource.getId());

		// at the iterate to the already-seen-set with the result IDs, so that we can transform
		// the feedback edges and let them stop when encountering the iterate node
		//将反馈转换对象以及其对应的结果集合的映射关系加入已遍历的Map中，这样在进行反馈边转换时，当它们向上递归转换时
		//遇到当前的反馈转换对象将停止递归转换
		alreadyTransformed.put(iterate, resultIds);

		// so that we can determine the slot sharing group from all feedback edges
		//遍历迭代的所有反馈边，并将所有反馈边对应的转换对象编号加入allFeedbackIds中
		List<Integer> allFeedbackIds = new ArrayList<>();

		for (Transformation<T> feedbackEdge : iterate.getFeedbackEdges()) {
			//对反馈边转换对象执行递归转换
			Collection<Integer> feedbackIds = transform(feedbackEdge);
			//将获取到的反馈边转换对象编号集合加入allFeedbackIds
			allFeedbackIds.addAll(feedbackIds);
			//遍历所有的反馈转换对象的编号，并在StreamGraph中构建从反馈转换对象到迭代sink之间的边
			for (Integer feedbackId: feedbackIds) {
				streamGraph.addEdge(feedbackId,
						itSink.getId(),
						0
				);
			}
		}

		//决定所有的反馈对象的”槽共享组“名
		String slotSharingGroup = determineSlotSharingGroup(null, allFeedbackIds);
		// slot sharing group of iteration node must exist
		if (slotSharingGroup == null) {
			slotSharingGroup = "SlotSharingGroup-" + iterate.getId();
		}

		//为迭代sink设置槽共享组名称
		itSink.setSlotSharingGroup(slotSharingGroup);
		//为迭代source设置槽共享组名称
		itSource.setSlotSharingGroup(slotSharingGroup);
		//返回该转换对象对应的编号结果集
		return resultIds;
	}

	/**
	 * Transforms a {@code CoFeedbackTransformation}.
	 *
	 * <p>This will only transform feedback edges, the result of this transform will be wired
	 * to the second input of a Co-Transform. The original input is wired directly to the first
	 * input of the downstream Co-Transform.
	 *
	 * <p>This is responsible for creating the IterationSource and IterationSink which
	 * are used to feed back the elements.
	 */
	private <F> Collection<Integer> transformCoFeedback(CoFeedbackTransformation<F> coIterate) {

		// For Co-Iteration we don't need to transform the input and wire the input to the
		// head operator by returning the input IDs, the input is directly wired to the left
		// input of the co-operation. This transform only needs to return the ids of the feedback
		// edges, since they need to be wired to the second input of the co-operation.

		// create the fake iteration source/sink pair
		Tuple2<StreamNode, StreamNode> itSourceAndSink = streamGraph.createIterationSourceAndSink(
				coIterate.getId(),
				getNewIterationNodeId(),
				getNewIterationNodeId(),
				coIterate.getWaitTime(),
				coIterate.getParallelism(),
				coIterate.getMaxParallelism(),
				coIterate.getMinResources(),
				coIterate.getPreferredResources());

		StreamNode itSource = itSourceAndSink.f0;
		StreamNode itSink = itSourceAndSink.f1;

		// We set the proper serializers for the sink/source
		streamGraph.setSerializers(itSource.getId(), null, null, coIterate.getOutputType().createSerializer(executionConfig));
		streamGraph.setSerializers(itSink.getId(), coIterate.getOutputType().createSerializer(executionConfig), null, null);

		Collection<Integer> resultIds = Collections.singleton(itSource.getId());

		// at the iterate to the already-seen-set with the result IDs, so that we can transform
		// the feedback edges and let them stop when encountering the iterate node
		alreadyTransformed.put(coIterate, resultIds);

		// so that we can determine the slot sharing group from all feedback edges
		List<Integer> allFeedbackIds = new ArrayList<>();

		for (Transformation<F> feedbackEdge : coIterate.getFeedbackEdges()) {
			Collection<Integer> feedbackIds = transform(feedbackEdge);
			allFeedbackIds.addAll(feedbackIds);
			for (Integer feedbackId: feedbackIds) {
				streamGraph.addEdge(feedbackId,
						itSink.getId(),
						0
				);
			}
		}

		String slotSharingGroup = determineSlotSharingGroup(null, allFeedbackIds);

		itSink.setSlotSharingGroup(slotSharingGroup);
		itSource.setSlotSharingGroup(slotSharingGroup);

		return Collections.singleton(itSource.getId());
	}

	/**
	 * Transforms a {@code SourceTransformation}.
	 */
	private <T> Collection<Integer> transformSource(SourceTransformation<T> source) {
		String slotSharingGroup = determineSlotSharingGroup(source.getSlotSharingGroup(), Collections.emptyList());

		streamGraph.addSource(source.getId(),
				slotSharingGroup,
				source.getCoLocationGroupKey(),
				source.getOperatorFactory(),
				null,
				source.getOutputType(),
				"Source: " + source.getName());
		if (source.getOperatorFactory() instanceof InputFormatOperatorFactory) {
			streamGraph.setInputFormat(source.getId(),
					((InputFormatOperatorFactory<T>) source.getOperatorFactory()).getInputFormat());
		}
		int parallelism = source.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ?
			source.getParallelism() : executionConfig.getParallelism();
		streamGraph.setParallelism(source.getId(), parallelism);
		streamGraph.setMaxParallelism(source.getId(), source.getMaxParallelism());
		return Collections.singleton(source.getId());
	}

	/**
	 * Transforms a {@code SourceTransformation}.
	 */
	private <T> Collection<Integer> transformSink(SinkTransformation<T> sink) {

		Collection<Integer> inputIds = transform(sink.getInput());

		String slotSharingGroup = determineSlotSharingGroup(sink.getSlotSharingGroup(), inputIds);

		streamGraph.addSink(sink.getId(),
				slotSharingGroup,
				sink.getCoLocationGroupKey(),
				sink.getOperatorFactory(),
				sink.getInput().getOutputType(),
				null,
				"Sink: " + sink.getName());

		StreamOperatorFactory operatorFactory = sink.getOperatorFactory();
		if (operatorFactory instanceof OutputFormatOperatorFactory) {
			streamGraph.setOutputFormat(sink.getId(), ((OutputFormatOperatorFactory) operatorFactory).getOutputFormat());
		}

		int parallelism = sink.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ?
			sink.getParallelism() : executionConfig.getParallelism();
		streamGraph.setParallelism(sink.getId(), parallelism);
		streamGraph.setMaxParallelism(sink.getId(), sink.getMaxParallelism());

		for (Integer inputId: inputIds) {
			streamGraph.addEdge(inputId,
					sink.getId(),
					0
			);
		}

		if (sink.getStateKeySelector() != null) {
			TypeSerializer<?> keySerializer = sink.getStateKeyType().createSerializer(executionConfig);
			streamGraph.setOneInputStateKey(sink.getId(), sink.getStateKeySelector(), keySerializer);
		}

		return Collections.emptyList();
	}

	/**
	 * Transforms a {@code OneInputTransformation}.
	 *
	 * <p>This recursively transforms the inputs, creates a new {@code StreamNode} in the graph and
	 * wired the inputs to this new node.
	 */
	private <IN, OUT> Collection<Integer> transformOneInputTransform(OneInputTransformation<IN, OUT> transform) {

		Collection<Integer> inputIds = transform(transform.getInput());

		// the recursive call might have already transformed this
		// 在递归处理节点过程中，某个节点可能已经被其他子节点先处理过了，需要跳过
		if (alreadyTransformed.containsKey(transform)) {
			return alreadyTransformed.get(transform);
		}

		// 在给StreamGraph创建并添加一个operator时，需要给该operator指定slotSharingGroup，
		// 这时需要调用方法determineSlotSharingGroup来获得SlotSharingGroup的名称.
		// 这个group用来定义当前我们在处理的这个操作符可以跟什么操作符chain到一个slot里进行操作
		// 因为有时候我们可能不满意flink替我们做的chain聚合
		// 一个slot就是一个执行task的基本容器
		String slotSharingGroup = determineSlotSharingGroup(transform.getSlotSharingGroup(), inputIds);

		//把该operator加入图
		streamGraph.addOperator(transform.getId(),
				slotSharingGroup,
				transform.getCoLocationGroupKey(),
				transform.getOperatorFactory(),
				transform.getInputType(),
				transform.getOutputType(),
				transform.getName());

		//对于keyedStream，我们还要记录它的keySelector方法
		//flink并不真正为每个keyedStream保存一个key，而是每次需要用到key的时候都使用keySelector方法进行计算
		if (transform.getStateKeySelector() != null) {
			TypeSerializer<?> keySerializer = transform.getStateKeyType().createSerializer(executionConfig);
			streamGraph.setOneInputStateKey(transform.getId(), transform.getStateKeySelector(), keySerializer);
		}

		int parallelism = transform.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ?
			transform.getParallelism() : executionConfig.getParallelism();
		streamGraph.setParallelism(transform.getId(), parallelism);
		streamGraph.setMaxParallelism(transform.getId(), transform.getMaxParallelism());

		//为当前节点和它的依赖节点建立边
		for (Integer inputId: inputIds) {
			streamGraph.addEdge(inputId, transform.getId(), 0);
		}

		return Collections.singleton(transform.getId());
	}

	/**
	 * Transforms a {@code TwoInputTransformation}.
	 *
	 * <p>This recursively transforms the inputs, creates a new {@code StreamNode} in the graph and
	 * wired the inputs to this new node.
	 */
	private <IN1, IN2, OUT> Collection<Integer> transformTwoInputTransform(TwoInputTransformation<IN1, IN2, OUT> transform) {

		Collection<Integer> inputIds1 = transform(transform.getInput1());
		Collection<Integer> inputIds2 = transform(transform.getInput2());

		// the recursive call might have already transformed this
		if (alreadyTransformed.containsKey(transform)) {
			return alreadyTransformed.get(transform);
		}

		List<Integer> allInputIds = new ArrayList<>();
		allInputIds.addAll(inputIds1);
		allInputIds.addAll(inputIds2);

		String slotSharingGroup = determineSlotSharingGroup(transform.getSlotSharingGroup(), allInputIds);

		streamGraph.addCoOperator(
				transform.getId(),
				slotSharingGroup,
				transform.getCoLocationGroupKey(),
				transform.getOperatorFactory(),
				transform.getInputType1(),
				transform.getInputType2(),
				transform.getOutputType(),
				transform.getName());

		if (transform.getStateKeySelector1() != null || transform.getStateKeySelector2() != null) {
			TypeSerializer<?> keySerializer = transform.getStateKeyType().createSerializer(executionConfig);
			streamGraph.setTwoInputStateKey(transform.getId(), transform.getStateKeySelector1(), transform.getStateKeySelector2(), keySerializer);
		}

		int parallelism = transform.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ?
			transform.getParallelism() : executionConfig.getParallelism();
		streamGraph.setParallelism(transform.getId(), parallelism);
		streamGraph.setMaxParallelism(transform.getId(), transform.getMaxParallelism());

		for (Integer inputId: inputIds1) {
			streamGraph.addEdge(inputId,
					transform.getId(),
					1
			);
		}

		for (Integer inputId: inputIds2) {
			streamGraph.addEdge(inputId,
					transform.getId(),
					2
			);
		}

		return Collections.singleton(transform.getId());
	}

	/**
	 * Determines the slot sharing group for an operation based on the slot sharing group set by
	 * the user and the slot sharing groups of the inputs.
	 *
	 * <p>If the user specifies a group name, this is taken as is. If nothing is specified and
	 * the input operations all have the same group name then this name is taken. Otherwise the
	 * default group is chosen.
	 *
	 * @param specifiedGroup The group specified by the user.
	 * @param inputIds The IDs of the input operations.
	 */
	private String determineSlotSharingGroup(String specifiedGroup, Collection<Integer> inputIds) {
		if (!isSlotSharingEnabled) {
			return null;
		}

		// 当用户指定了组名，则直接使用用户指定的名称
		if (specifiedGroup != null) {
			return specifiedGroup;
		} else {
			// 如果用户没有指定特定的名称，则需要结合输入节点来做决定：第一种情况如果所有的输入节点都拥有相同的slotSharingGroup名称，
			// 那么就使用该组名；否则组名将被命名为default
			String inputGroup = null;
			for (int id: inputIds) {
				String inputGroupCandidate = streamGraph.getSlotSharingGroup(id);
				if (inputGroup == null) {
					inputGroup = inputGroupCandidate;
				} else if (!inputGroup.equals(inputGroupCandidate)) {
					return DEFAULT_SLOT_SHARING_GROUP;
				}
			}
			return inputGroup == null ? DEFAULT_SLOT_SHARING_GROUP : inputGroup;
		}
	}

	private <T> void validateSplitTransformation(Transformation<T> input) {
		if (input instanceof SelectTransformation || input instanceof SplitTransformation) {
			throw new IllegalStateException("Consecutive multiple splits are not supported. Splits are deprecated. Please use side-outputs.");
		} else if (input instanceof SideOutputTransformation) {
			throw new IllegalStateException("Split after side-outputs are not supported. Splits are deprecated. Please use side-outputs.");
		} else if (input instanceof UnionTransformation) {
			for (Transformation<T> transformation : ((UnionTransformation<T>) input).getInputs()) {
				validateSplitTransformation(transformation);
			}
		} else if (input instanceof PartitionTransformation) {
			validateSplitTransformation(((PartitionTransformation) input).getInput());
		} else {
			return;
		}
	}
}
