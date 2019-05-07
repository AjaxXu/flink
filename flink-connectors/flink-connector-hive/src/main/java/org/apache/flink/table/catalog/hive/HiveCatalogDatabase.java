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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A hive catalog database implementation.
 */
public class HiveCatalogDatabase implements CatalogDatabase {
	// Property of the database
	private final Map<String, String> properties;
	// HDFS path of the database
	private String location;
	// Comment of the database
	private String comment = "This is a hive catalog database.";

	public HiveCatalogDatabase() {
		properties = new HashMap<>();
	}

	public HiveCatalogDatabase(Map<String, String> properties) {
		this.properties = checkNotNull(properties, "properties cannot be null");
	}

	public HiveCatalogDatabase(Map<String, String> properties, String comment) {
		this(properties);
		this.comment = checkNotNull(comment, "comment cannot be null");
	}

	public HiveCatalogDatabase(Map<String, String> properties, String location, String comment) {
		this(properties, comment);

		checkArgument(!StringUtils.isNullOrWhitespaceOnly(location), "location cannot be null or empty");
		this.location = location;
	}

	@Override
	public Map<String, String> getProperties() {
		return properties;
	}

	@Override
	public String getComment() {
		return comment;
	}

	@Override
	public HiveCatalogDatabase copy() {
		return new HiveCatalogDatabase(new HashMap<>(properties), location, comment);
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.of(comment);
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return Optional.of("This is a Hive catalog database stored in memory only");
	}

	public String getLocation() {
		return location;
	}
}
