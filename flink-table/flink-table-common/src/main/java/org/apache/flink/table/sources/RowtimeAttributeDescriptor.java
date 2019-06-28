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

package org.apache.flink.table.sources;

import org.apache.flink.table.sources.tsextractors.TimestampExtractor;
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;

import java.util.Objects;

/**
 * 描述{@link TableSource}的rowtime属性，包括属性名、timestamp抽取器、watermark策略
 * Describes a rowtime attribute of a {@link TableSource}.
 */
public final class RowtimeAttributeDescriptor {

	private final String attributeName;
	private final TimestampExtractor timestampExtractor;
	private final WatermarkStrategy watermarkStrategy;

	public RowtimeAttributeDescriptor(
			String attributeName,
			TimestampExtractor timestampExtractor,
			WatermarkStrategy watermarkStrategy) {
		this.attributeName = attributeName;
		this.timestampExtractor = timestampExtractor;
		this.watermarkStrategy = watermarkStrategy;
	}

	/** Returns the name of the rowtime attribute. */
	public String getAttributeName() {
		return attributeName;
	}

	/** Returns the [[TimestampExtractor]] for the attribute. */
	public TimestampExtractor getTimestampExtractor() {
		return timestampExtractor;
	}

	/** Returns the [[WatermarkStrategy]] for the attribute. */
	public WatermarkStrategy getWatermarkStrategy() {
		return watermarkStrategy;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		RowtimeAttributeDescriptor that = (RowtimeAttributeDescriptor) o;
		return Objects.equals(attributeName, that.attributeName) &&
			Objects.equals(timestampExtractor, that.timestampExtractor) &&
			Objects.equals(watermarkStrategy, that.watermarkStrategy);
	}

	@Override
	public int hashCode() {
		return Objects.hash(attributeName, timestampExtractor, watermarkStrategy);
	}
}
