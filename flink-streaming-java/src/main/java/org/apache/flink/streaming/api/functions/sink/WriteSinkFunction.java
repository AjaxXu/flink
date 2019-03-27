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

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.annotation.PublicEvolving;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;

/**
 * WriteSinkFunction是一个抽象类。
 * 该类的主要作用是将需要输出的tuples（元组）作为简单的文本输出到指定路径的文件中去，元组被收集到一个list中去，然后周期性得写入文件。
 * Simple implementation of the SinkFunction writing tuples as simple text to
 * the file specified by path. Tuples are collected to a list and written to the
 * file periodically. The file specified by path is created if it does not
 * exist, cleared if it exists before the writing.
 *
 * @param <IN>
 *            Input tuple type
 *
 * @deprecated Please use the {@code BucketingSink} for writing to files from a streaming program.
 */
@PublicEvolving
@Deprecated
public abstract class WriteSinkFunction<IN> implements SinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	protected final String path;
	protected ArrayList<IN> tupleList = new ArrayList<IN>();
	protected WriteFormat<IN> format;

	public WriteSinkFunction(String path, WriteFormat<IN> format) {
		this.path = path;
		this.format = format;
		cleanFile(path);
	}

	/**
	 * Creates target file if it does not exist, cleans it if it exists.
	 *
	 * @param path
	 *            is the path to the location where the tuples are written
	 */
	protected void cleanFile(String path) {
		try {
			PrintWriter writer;
			writer = new PrintWriter(path);
			writer.print("");
			writer.close();
		} catch (FileNotFoundException e) {
			throw new RuntimeException("An error occurred while cleaning the file: " + e.getMessage(), e);
		}
	}

	/**
	 * Condition for writing the contents of tupleList and clearing it.
	 *
	 * @return value of the updating condition
	 */
	protected abstract boolean updateCondition();

	/**
	 * Statements to be executed after writing a batch goes here.
	 */
	protected abstract void resetParameters();

	/**
	 * Implementation of the invoke method of the SinkFunction class. Collects
	 * the incoming tuples in tupleList and appends the list to the end of the
	 * target file if updateCondition() is true or the current tuple is the
	 * endTuple.
	 * 从实现来看，其先将需要sink的元组加入内部集合。然后调用updateCondition方法。
	 * 该方法是WriteSinkFunction定义的抽象方法。用于实现判断将tupleList写入文件以及清空tupleList的条件。
	 * 接着将集合中的tuple写入到指定的文件中。
	 * 最后又调用了resetParameters方法。该方法同样是一个抽象方法，它的主要用途是当写入的场景是批量写入时，
	 * 可能会有一些状态参数，该方法就是用于对状态进行reset
	 */
	@Override
	public void invoke(IN tuple) {

		tupleList.add(tuple);
		if (updateCondition()) {
			format.write(path, tupleList);
			resetParameters();
		}

	}

}
