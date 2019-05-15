package org.apache.flink.streaming.examples.sideoutput;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * @author Louis
 */
public class ProcessFunctionExample {

	private static final OutputTag<String> gt10Tag = new OutputTag<String>("gt10") {};


	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStreamSource<String> source = env.fromElements(WordCountData.WORDS);

		SingleOutputStreamOperator<String> withTimestampsAndWatermarks = source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
			@Override
			public long extractAscendingTimestamp(String element) {
				return System.currentTimeMillis();
			}
		});
		SingleOutputStreamOperator<Tuple2<String, Long>> mainStream = withTimestampsAndWatermarks.flatMap(new FlatMapFunction<String, WordWithCount>() {
			@Override
			public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
				String[] values = value.split(" ");
				for (String v : values) {
					out.collect(new WordWithCount(v, 1));
				}
			}
		}).keyBy("word")
			.process(new MyProcessFunction());
		DataStream<String> sideOutput = mainStream.getSideOutput(gt10Tag);
		sideOutput.print();
		env.execute("Process");
	}

	public static class MyProcessFunction extends ProcessFunction<WordWithCount, Tuple2<String, Long>> {

		private final ValueStateDescriptor<CounterWithTS> stateDesc = new ValueStateDescriptor<CounterWithTS>("myState", CounterWithTS.class);

		@Override
		public void processElement(WordWithCount value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
			ValueState<CounterWithTS> state = getRuntimeContext().getState(stateDesc);
			CounterWithTS current = state.value();
			if (current == null) {
				current = new CounterWithTS();
				current.key = value.word;
			}
			current.count++;
			current.lastModified = ctx.timestamp();
			state.update(current);
			TimerService timerService = ctx.timerService();
			timerService.registerEventTimeTimer(current.lastModified + 100L);
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
			CounterWithTS current = getRuntimeContext().getState(stateDesc).value();
			if (current.lastModified == timestamp) {
				out.collect(new Tuple2<String, Long>(current.key, current.count));
			} else if (current.count > 10) {
				ctx.output(gt10Tag, current.key);
			}
		}
	}

	public static class WordWithCount {

		public String word;
		public long count;

		public WordWithCount() {}

		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return word + " : " + count;
		}
	}

	private static class CounterWithTS {
		public String key;
		public long count;
		public Long lastModified;

		public CounterWithTS() {
		}

		public CounterWithTS(String key, int count, Long lastModified) {
			this.key = key;
			this.count = count;
			this.lastModified = lastModified;
		}
	}
}
