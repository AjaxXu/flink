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

package org.apache.flink.table.runtime.hashtable;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Bucket area for hash table.
 * Hash Table的bucket区域
 *
 * <p>The layout of the buckets inside a memory segment is as follows:</p>
 * <pre>
 * +----------------------------- Bucket x ----------------------------
 * |element count (2 bytes) | probedFlags (2 bytes) | next-bucket-in-chain-pointer (4 bytes) |
 * |
 * |hashCode 1 (4 bytes) | hashCode 2 (4 bytes) | hashCode 3 (4 bytes) |
 * | ... hashCode n-1 (4 bytes) | hashCode n (4 bytes)
 * |
 * |pointer 1 (4 bytes) | pointer 2 (4 bytes) | pointer 3 (4 bytes) |
 * | ... pointer n-1 (4 bytes) | pointer n (4 bytes)
 * |
 * +---------------------------- Bucket x + 1--------------------------
 * | ...
 * |
 * </pre>
 */
public class BinaryHashBucketArea {

	private static final Logger LOG = LoggerFactory.getLogger(BinaryHashBucketArea.class);

	/**
	 * Log 2 of bucket size.
	 */
	static final int BUCKET_SIZE_BITS = 7;

	/**
	 * 128, bucket size of bytes.
	 */
	static final int BUCKET_SIZE = 0x1 << BUCKET_SIZE_BITS;

	/**
	 * The length of the hash code stored in the bucket.
	 */
	static final int HASH_CODE_LEN = 4;

	/**
	 * The length of a pointer from a hash bucket to the record in the buffers.
	 */
	static final int POINTER_LEN = 4;

	/**
	 * The number of bytes that the entry in the hash structure occupies, in bytes.
	 * It corresponds to a 4 byte hash value and an 4 byte pointer.
	 */
	public static final int RECORD_BYTES = HASH_CODE_LEN + POINTER_LEN;

	/**
	 * Offset of the field in the bucket header indicating the bucket's element count.
	 * 指示bucket元素数量在bucket头中的offset
	 */
	static final int HEADER_COUNT_OFFSET = 0;

	/**
	 * Offset of the field in the bucket header that holds the probed bit set.
	 */
	static final int PROBED_FLAG_OFFSET = 2;

	/**
	 * Offset of the field in the bucket header that holds the forward pointer to its
	 * first overflow bucket.
	 */
	static final int HEADER_FORWARD_OFFSET = 4;

	/**
	 * Total length for bucket header.
	 */
	static final int BUCKET_HEADER_LENGTH = 8;

	/**
	 * The maximum number of elements that can be loaded in a bucket.
	 * 15
	 */
	static final int NUM_ENTRIES_PER_BUCKET = (BUCKET_SIZE - BUCKET_HEADER_LENGTH) / RECORD_BYTES;

	/**
	 * Offset of record pointer.
	 */
	static final int BUCKET_POINTER_START_OFFSET = BUCKET_HEADER_LENGTH + HASH_CODE_LEN * NUM_ENTRIES_PER_BUCKET;

	/**
	 * Constant for the forward pointer, indicating that the pointer is not set.
	 * 指示下一个bucket的指针还没设置
	 */
	static final int BUCKET_FORWARD_POINTER_NOT_SET = 0xFFFFFFFF;

	/**
	 * Constant for the bucket header to init. (count: 0, probedFlag: 0, forwardPointerNotSet: ~0x0)
	 */
	private static final long BUCKET_HEADER_INIT;

	static {
		if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
			BUCKET_HEADER_INIT = 0xFFFFFFFF00000000L;
		} else {
			BUCKET_HEADER_INIT = 0x00000000FFFFFFFFL;
		}
	}

	/**
	 * The load factor used when none specified in constructor.
	 */
	private static final double DEFAULT_LOAD_FACTOR = 0.75;

	final BinaryHashTable table;
	private final double estimatedRowCount;
	private final double loadFactor;
	BinaryHashPartition partition;
	private int size;

	MemorySegment[] buckets;
	int numBuckets;
	private int numBucketsMask;
	// segments in which overflow buckets from the table structure are stored
	MemorySegment[] overflowSegments;
	int numOverflowSegments; // the number of actual segments in the overflowSegments array
	private int nextOverflowBucket; // the next free bucket in the current overflow segment
	private int threshold;

	private boolean inReHash = false;

	BinaryHashBucketArea(BinaryHashTable table, double estimatedRowCount, int maxSegs) {
		this(table, estimatedRowCount, maxSegs, DEFAULT_LOAD_FACTOR);
	}

	private BinaryHashBucketArea(BinaryHashTable table, double estimatedRowCount, int maxSegs, double loadFactor) {
		this.table = table;
		this.estimatedRowCount = estimatedRowCount;
		this.loadFactor = loadFactor;
		this.size = 0;

		// 最少bucket数量
		int minNumBuckets = (int) Math.ceil((estimatedRowCount / loadFactor / NUM_ENTRIES_PER_BUCKET));
		// 用于存放bucket的segment数量
		int bucketNumSegs = Math.max(1, Math.min(maxSegs, (minNumBuckets >>> table.bucketsPerSegmentBits) +
				((minNumBuckets & table.bucketsPerSegmentMask) == 0 ? 0 : 1)));
		// bucket数量
		int numBuckets = MathUtils.roundDownToPowerOf2(bucketNumSegs << table.bucketsPerSegmentBits);

		// entry的阈值: numBuckets * 15 * 0.75
		int threshold = (int) (numBuckets * NUM_ENTRIES_PER_BUCKET * loadFactor);

		MemorySegment[] buckets = new MemorySegment[bucketNumSegs];
		table.ensureNumBuffersReturned(bucketNumSegs);

		// go over all segments that are part of the table
		for (int i = 0; i < bucketNumSegs; i++) {
			// 从table中获取segment
			final MemorySegment seg = table.getNextBuffer();
			// 初始化segment：遍历每个bucket，初始化bucket header
			initMemorySegment(seg);
			buckets[i] = seg;
		}

		setNewBuckets(buckets, numBuckets, threshold);
	}

	private void setNewBuckets(MemorySegment[] buckets, int numBuckets, int threshold) {
		this.buckets = buckets;
		checkArgument(MathUtils.isPowerOf2(numBuckets));
		this.numBuckets = numBuckets;
		this.numBucketsMask = numBuckets - 1;
		this.overflowSegments = new MemorySegment[2]; // 存放溢出的bucket的segment
		this.numOverflowSegments = 0;
		this.nextOverflowBucket = 0;
		this.threshold = threshold;
	}

	public void setPartition(BinaryHashPartition partition) {
		this.partition = partition;
	}

	private void resize(boolean spillingAllowed) throws IOException {
		MemorySegment[] oldBuckets = this.buckets;
		int oldNumBuckets = numBuckets;
		MemorySegment[] oldOverflowSegments = overflowSegments;
		int newNumSegs = oldBuckets.length * 2;
		int newNumBuckets = MathUtils.roundDownToPowerOf2(newNumSegs << table.bucketsPerSegmentBits);
		int newThreshold = (int) (newNumBuckets * NUM_ENTRIES_PER_BUCKET * loadFactor);

		// We can't resize if not spillingAllowed and there are not enough buffers.
		if (!spillingAllowed && newNumSegs > table.remainBuffers()) {
			return;
		}

		// request new buckets.
		MemorySegment[] newBuckets = new MemorySegment[newNumSegs];
		for (int i = 0; i < newNumSegs; i++) {
			MemorySegment seg = table.getNextBuffer();
			if (seg == null) {
				final int spilledPart = table.spillPartition();
				if (spilledPart == partition.partitionNumber) {
					// this bucket is no longer in-memory
					// free new segments.
					for (int j = 0; j < i; j++) {
						table.free(newBuckets[j]);
					}
					return;
				}
				seg = table.getNextBuffer();
				if (seg == null) {
					throw new RuntimeException(
							"Bug in HybridHashJoin: No memory became available after spilling a partition.");
				}
			}
			initMemorySegment(seg);
			newBuckets[i] = seg;
		}

		setNewBuckets(newBuckets, newNumBuckets, newThreshold);

		reHash(oldBuckets, oldNumBuckets, oldOverflowSegments);
	}

	private void reHash(
			MemorySegment[] oldBuckets,
			int oldNumBuckets,
			MemorySegment[] oldOverflowSegments) throws IOException {
		long reHashStartTime = System.currentTimeMillis();
		inReHash = true;
		int scanCount = -1;
		while (true) {
			scanCount++;
			if (scanCount >= oldNumBuckets) {
				break;
			}
			// move to next bucket, update all the current bucket status with new bucket information.
			final int bucketArrayPos = scanCount >> table.bucketsPerSegmentBits;
			int bucketInSegOffset = (scanCount & table.bucketsPerSegmentMask) << BUCKET_SIZE_BITS;
			MemorySegment bucketSeg = oldBuckets[bucketArrayPos];

			int countInBucket = bucketSeg.getShort(bucketInSegOffset + HEADER_COUNT_OFFSET);
			int numInBucket = 0;
			while (countInBucket != 0) {
				int hashCodeOffset = bucketInSegOffset + BUCKET_HEADER_LENGTH;
				int pointerOffset = bucketInSegOffset + BUCKET_POINTER_START_OFFSET;
				while (numInBucket < countInBucket) {
					int hashCode = bucketSeg.getInt(hashCodeOffset);
					int pointer = bucketSeg.getInt(pointerOffset);
					if (!insertToBucket(hashCode, pointer, true, false)) {
						buildBloomFilterAndFree(oldBuckets, oldNumBuckets, oldOverflowSegments);
						return;
					}
					numInBucket++;
					hashCodeOffset += HASH_CODE_LEN;
					pointerOffset += POINTER_LEN;
				}

				// this segment is done. check if there is another chained bucket
				int forwardPointer = bucketSeg.getInt(bucketInSegOffset + HEADER_FORWARD_OFFSET);
				if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
					break;
				}

				final int overflowSegIndex = forwardPointer >>> table.segmentSizeBits;
				bucketSeg = oldOverflowSegments[overflowSegIndex];
				bucketInSegOffset = forwardPointer & table.segmentSizeMask;
				countInBucket = bucketSeg.getShort(bucketInSegOffset + HEADER_COUNT_OFFSET);
				numInBucket = 0;
			}
		}

		freeMemory(oldBuckets, oldOverflowSegments);
		inReHash = false;
		LOG.info("The rehash take {} ms for {} segments", (System.currentTimeMillis() - reHashStartTime), numBuckets);
	}

	private void freeMemory(MemorySegment[] buckets, MemorySegment[] overflowSegments) {
		for (MemorySegment segment : buckets) {
			table.free(segment);
		}
		for (MemorySegment segment : overflowSegments) {
			if (segment != null) {
				table.free(segment);
			}
		}
	}

	private void initMemorySegment(MemorySegment seg) {
		// go over all buckets in the segment
		for (int k = 0; k < table.bucketsPerSegment; k++) {
			final int bucketOffset = k * BUCKET_SIZE;

			// init count and probeFlag and forward pointer together.
			// 初始化bucket header
			seg.putLong(bucketOffset + HEADER_COUNT_OFFSET, BUCKET_HEADER_INIT);
		}
	}

	private boolean insertToBucket(
			MemorySegment bucketSeg, int bucketInSegmentPos,
			int hashCode, int pointer, boolean spillingAllowed, boolean sizeAddAndCheckResize) throws IOException {
		// count为bucket中entry数量
		final int count = bucketSeg.getShort(bucketInSegmentPos + HEADER_COUNT_OFFSET);
		if (count < NUM_ENTRIES_PER_BUCKET) { // count < 15
			// we are good in our current bucket, put the values
			// 更新count
			bucketSeg.putShort(bucketInSegmentPos + HEADER_COUNT_OFFSET, (short) (count + 1)); // update count
			// 设置这次插入entry的hashCode
			bucketSeg.putInt(bucketInSegmentPos + BUCKET_HEADER_LENGTH + (count * HASH_CODE_LEN), hashCode);    // hash code
			// 设置这次插入entry的pointer
			bucketSeg.putInt(bucketInSegmentPos + BUCKET_POINTER_START_OFFSET + (count * POINTER_LEN), pointer); // pointer
		} else {
			// count >= 15, 该bucket放不下
			// we need to go to the overflow buckets
			// 找到溢出的下一个bucket
			final int originalForwardPointer = bucketSeg.getInt(bucketInSegmentPos + HEADER_FORWARD_OFFSET);
			final int forwardForNewBucket;

			if (originalForwardPointer != BUCKET_FORWARD_POINTER_NOT_SET) {
				// 下一个bucket已经设置

				// forward pointer set
				final int overflowSegIndex = originalForwardPointer >>> table.segmentSizeBits;
				final int segOffset = originalForwardPointer & table.segmentSizeMask;
				final MemorySegment seg = overflowSegments[overflowSegIndex];

				// overflow中对应bucket的entry数量
				final short obCount = seg.getShort(segOffset + HEADER_COUNT_OFFSET);

				// check if there is space in this overflow bucket
				if (obCount < NUM_ENTRIES_PER_BUCKET) {
					// 如果该overflow bucket中还有空间
					// space in this bucket and we are done
					seg.putShort(segOffset + HEADER_COUNT_OFFSET, (short) (obCount + 1)); // update count
					seg.putInt(segOffset + BUCKET_HEADER_LENGTH + (obCount * HASH_CODE_LEN), hashCode);    // hash code
					seg.putInt(segOffset + BUCKET_POINTER_START_OFFSET + (obCount * POINTER_LEN), pointer); // pointer
					// 返回插入成功
					return true;
				} else {
					// no space here, we need a new bucket. this current overflow bucket will be the
					// target of the new overflow bucket
					// 没有空间，则当前overflow bucket将是新的overflow bucket的目标.
					// (头插法，这样在未溢出bucket那里，一跳就能跳到新的overflow bucket)
					forwardForNewBucket = originalForwardPointer;
				}
			} else {
				// no overflow bucket yet, so we need a first one
				// 还没设置，需要先设置一个
				forwardForNewBucket = BUCKET_FORWARD_POINTER_NOT_SET;
			}

			// we need a new overflow bucket
			MemorySegment overflowSeg;
			final int overflowBucketNum;
			final int overflowBucketOffset;

			// first, see if there is space for an overflow bucket remaining in the last overflow segment
			// 先检查上一个overflow segment中是否还有空间
			if (nextOverflowBucket == 0) {
				// no space left in last bucket, or no bucket yet, so create an overflow segment
				// 没有空间，则创建一个overflow segment
				overflowSeg = table.getNextBuffer();
				if (overflowSeg == null) {
					// 没有可用的segment，需要溢出partition
					// no memory available to create overflow bucket. we need to spill a partition
					if (!spillingAllowed) {
						// 不允许溢出，直接抛出异常
						throw new IOException("Hashtable memory ran out in a non-spillable situation. " +
								"This is probably related to wrong size calculations.");
					}
					final int spilledPart = table.spillPartition();
					// 如果溢出的partition正好和当前partition一样，说明该bucket不在内存中，返回插入失败
					if (spilledPart == partition.partitionNumber) {
						// this bucket is no longer in-memory
						return false;
					}
					overflowSeg = table.getNextBuffer();
					if (overflowSeg == null) {
						// 如果溢出一个partition仍然没有可用的segment，抛出一个bug
						throw new RuntimeException("Bug in HybridHashJoin: No memory became available after spilling a partition.");
					}
				}
				overflowBucketOffset = 0;
				overflowBucketNum = numOverflowSegments;

				// add the new overflow segment
				// 把得到的segment加入到overflowSegment数组中
				if (overflowSegments.length <= numOverflowSegments) {
					// 数组装满了，扩容为原来的2倍
					MemorySegment[] newSegsArray = new MemorySegment[overflowSegments.length * 2];
					System.arraycopy(overflowSegments, 0, newSegsArray, 0, overflowSegments.length);
					overflowSegments = newSegsArray;
				}
				overflowSegments[numOverflowSegments] = overflowSeg;
				numOverflowSegments++;
			} else {
				// there is space in the last overflow bucket
				// 最后一个overflow segment还有空间
				overflowBucketNum = numOverflowSegments - 1; // 可用的segment在数组中的index
				overflowSeg = overflowSegments[overflowBucketNum];
				overflowBucketOffset = nextOverflowBucket << BUCKET_SIZE_BITS;
			}

			// next overflow bucket is one ahead. if the segment is full, the next will be at the beginning
			// of a new segment
			// 下一个overflow bucket +1，如果segment满了，则设为0(新segment的第一个bucket)
			nextOverflowBucket = (nextOverflowBucket == table.bucketsPerSegmentMask ? 0 : nextOverflowBucket + 1);

			// insert the new overflow bucket in the chain of buckets
			// 1) set the old forward pointer
			// 2) let the bucket in the main table point to this one
			// 将新overflow bucket加入bucket链：1.在这个bucket设置旧的bucket 前进指针 2.主table中的bucket指向这个
			overflowSeg.putInt(overflowBucketOffset + HEADER_FORWARD_OFFSET, forwardForNewBucket);
			final int pointerToNewBucket = (overflowBucketNum << table.segmentSizeBits) + overflowBucketOffset;
			bucketSeg.putInt(bucketInSegmentPos + HEADER_FORWARD_OFFSET, pointerToNewBucket);

			// finally, insert the values into the overflow buckets
			overflowSeg.putInt(overflowBucketOffset + BUCKET_HEADER_LENGTH, hashCode);    // hash code
			overflowSeg.putInt(overflowBucketOffset + BUCKET_POINTER_START_OFFSET, pointer); // pointer

			// set the count to one
			overflowSeg.putShort(overflowBucketOffset + HEADER_COUNT_OFFSET, (short) 1);

			// initiate the probed bitset to 0.
			overflowSeg.putShort(overflowBucketOffset + PROBED_FLAG_OFFSET, (short) 0);
		}

		// 判断是否需要扩容
		if (sizeAddAndCheckResize && ++size > threshold) {
			resize(spillingAllowed);
		}
		return true;
	}

	private int findBucket(int hashCode) {
		return hashCode & this.numBucketsMask;
	}

	/**
	 * Insert into bucket by hashCode and pointer.
	 * @return return false when spill own partition.
	 */
	boolean insertToBucket(int hashCode, int pointer, boolean spillingAllowed, boolean sizeAddAndCheckResize) throws IOException {
		final int posHashCode = findBucket(hashCode);
		// get the bucket for the given hash code
		// bucket所在的segment在segment数组中的index
		final int bucketArrayPos = posHashCode >> table.bucketsPerSegmentBits;
		// bucket在segment中的位置
		final int bucketInSegmentPos = (posHashCode & table.bucketsPerSegmentMask) << BUCKET_SIZE_BITS;
		final MemorySegment bucket = this.buckets[bucketArrayPos];
		return insertToBucket(bucket, bucketInSegmentPos, hashCode, pointer, spillingAllowed, sizeAddAndCheckResize);
	}

	/**
	 * Append record and insert to bucket.
	 */
	boolean appendRecordAndInsert(BinaryRow record, int hashCode) throws IOException {
		final int posHashCode = findBucket(hashCode);
		// get the bucket for the given hash code
		final int bucketArrayPos = posHashCode >> table.bucketsPerSegmentBits;
		final int bucketInSegmentPos = (posHashCode & table.bucketsPerSegmentMask) << BUCKET_SIZE_BITS;
		final MemorySegment bucket = this.buckets[bucketArrayPos];

		if (!table.tryDistinctBuildRow ||
				!partition.isInMemory() ||
				!findFirstSameBuildRow(bucket, hashCode, bucketInSegmentPos, record)) {
			int pointer = partition.insertIntoBuildBuffer(record);
			if (pointer != -1) {
				// record was inserted into an in-memory partition. a pointer must be inserted into the buckets
				insertToBucket(bucket, bucketInSegmentPos, hashCode, pointer, true, true);
				return true;
			} else {
				return false;
			}
		} else {
			// distinct build rows in memory.
			return true;
		}
	}

	/**
	 * For distinct build.
	 */
	private boolean findFirstSameBuildRow(
			MemorySegment bucket,
			int searchHashCode,
			int bucketInSegmentOffset,
			BinaryRow buildRowToInsert) {
		int posInSegment = bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
		int countInBucket = bucket.getShort(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
		int numInBucket = 0;
		RandomAccessInputView view = partition.getBuildStateInputView();
		while (countInBucket != 0) {
			while (numInBucket < countInBucket) {

				final int thisCode = bucket.getInt(posInSegment);
				posInSegment += HASH_CODE_LEN;

				if (thisCode == searchHashCode) {
					final int pointer = bucket.getInt(bucketInSegmentOffset +
							BUCKET_POINTER_START_OFFSET + (numInBucket * POINTER_LEN));
					numInBucket++;
					try {
						view.setReadPosition(pointer);
						BinaryRow row = table.binaryBuildSideSerializer.mapFromPages(table.reuseBuildRow, view);
						if (buildRowToInsert.equals(row)) {
							return true;
						}
					} catch (IOException e) {
						throw new RuntimeException("Error deserializing key or value from the hashtable: " +
								e.getMessage(), e);
					}
				} else {
					numInBucket++;
				}
			}

			// this segment is done. check if there is another chained bucket
			final int forwardPointer = bucket.getInt(bucketInSegmentOffset + HEADER_FORWARD_OFFSET);
			if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
				return false;
			}

			final int overflowSegIndex = forwardPointer >>> table.segmentSizeBits;
			bucket = overflowSegments[overflowSegIndex];
			bucketInSegmentOffset = forwardPointer & table.segmentSizeMask;
			countInBucket = bucket.getShort(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
			posInSegment = bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
			numInBucket = 0;
		}
		return false;
	}

	/**
	 * Probe start lookup joined build rows.
	 */
	void startLookup(int hashCode) {
		final int posHashCode = findBucket(hashCode);

		// get the bucket for the given hash code
		final int bucketArrayPos = posHashCode >> table.bucketsPerSegmentBits;
		final int bucketInSegmentOffset = (posHashCode & table.bucketsPerSegmentMask) << BUCKET_SIZE_BITS;
		final MemorySegment bucket = this.buckets[bucketArrayPos];
		table.bucketIterator.set(bucket, overflowSegments, partition, hashCode, bucketInSegmentOffset);
	}

	void returnMemory(List<MemorySegment> target) {
		target.addAll(Arrays.asList(overflowSegments).subList(0, numOverflowSegments));
		target.addAll(Arrays.asList(buckets));
	}

	private void freeMemory() {
		table.availableMemory.addAll(Arrays.asList(overflowSegments).subList(0, numOverflowSegments));
		table.availableMemory.addAll(Arrays.asList(buckets));
	}

	/**
	 * Three situations:
	 * 1.Not use bloom filter, just free memory.
	 * 2.In rehash, free new memory and let rehash go build bloom filter from old memory.
	 * 3.Not in rehash and use bloom filter, build it and free memory.
	 */
	void buildBloomFilterAndFree() {
		if (inReHash || !table.useBloomFilters) {
			freeMemory();
		} else {
			buildBloomFilterAndFree(buckets, numBuckets, overflowSegments);
		}
	}

	private void buildBloomFilterAndFree(
			MemorySegment[] buckets,
			int numBuckets,
			MemorySegment[] overflowSegments) {
		if (table.useBloomFilters) {
			long numRecords = (long) Math.max(partition.getBuildSideRecordCount() * 1.5, estimatedRowCount);

			// BloomFilter size min of:
			// 1.remain buffers
			// 2.bf size for numRecords when fpp is 0.05
			// 3.max init bucket area buffers.
			int segSize = Math.min(
					Math.min(table.remainBuffers(),
					HashTableBloomFilter.optimalSegmentNumber(numRecords, table.pageSize(), 0.05)),
					table.maxInitBufferOfBucketArea(table.partitionsBeingBuilt.size()));

			if (segSize > 0) {
				HashTableBloomFilter filter = new HashTableBloomFilter(
						table.getNextBuffers(MathUtils.roundDownToPowerOf2(segSize)), numRecords);

				// Add all records to bloom filter.
				int scanCount = -1;
				while (true) {
					scanCount++;
					if (scanCount >= numBuckets) {
						break;
					}
					// move to next bucket, update all the current bucket status with new bucket information.
					final int bucketArrayPos = scanCount >> table.bucketsPerSegmentBits;
					int bucketInSegOffset = (scanCount & table.bucketsPerSegmentMask) << BUCKET_SIZE_BITS;
					MemorySegment bucketSeg = buckets[bucketArrayPos];

					int countInBucket = bucketSeg.getShort(bucketInSegOffset + HEADER_COUNT_OFFSET);
					int numInBucket = 0;
					while (countInBucket != 0) {
						int hashCodeOffset = bucketInSegOffset + BUCKET_HEADER_LENGTH;
						while (numInBucket < countInBucket) {
							filter.addHash(bucketSeg.getInt(hashCodeOffset));
							numInBucket++;
							hashCodeOffset += HASH_CODE_LEN;
						}

						// this segment is done. check if there is another chained bucket
						int forwardPointer = bucketSeg.getInt(bucketInSegOffset + HEADER_FORWARD_OFFSET);
						if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
							break;
						}

						final int overflowSegIndex = forwardPointer >>> table.segmentSizeBits;
						bucketSeg = overflowSegments[overflowSegIndex];
						bucketInSegOffset = forwardPointer & table.segmentSizeMask;
						countInBucket = bucketSeg.getShort(bucketInSegOffset + HEADER_COUNT_OFFSET);
						numInBucket = 0;
					}
				}

				partition.bloomFilter = filter;
			}
		}

		freeMemory(buckets, overflowSegments);
	}
}
