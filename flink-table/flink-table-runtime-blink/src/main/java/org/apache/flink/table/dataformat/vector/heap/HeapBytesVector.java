/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat.vector.heap;

import org.apache.flink.table.dataformat.vector.BytesColumnVector;

/**
 * This class supports string and binary data by value reference -- i.e. each field is
 * explicitly present, as opposed to provided by a dictionary reference.
 * In some cases, all the values will be in the same byte array to begin with,
 * but this need not be the case. If each value is in a separate byte
 * array to start with, or not all of the values are in the same original
 * byte array, you can still assign data by reference into this column vector.
 * This gives flexibility to use this in multiple situations.
 *
 * <p>When setting data by reference, the caller
 * is responsible for allocating the byte arrays used to hold the data.
 * You can also set data by value, as long as you call the initBuffer() method first.
 * You can mix "by value" and "by reference" in the same column vector,
 * though that use is probably not typical.
 */
public class HeapBytesVector extends AbstractHeapVector implements BytesColumnVector {

	private static final long serialVersionUID = -8529155738773478597L;

	/**
	 * start offset of each field.
	 */
	public int[] start;

	/**
	 * The length of each field.
	 */
	public int[] length;

	/**
	 * buffer to use when actually copying in data.
	 */
	public byte[] buffer;

	/**
	 * Hang onto a byte array for holding smaller byte values.
	 */
	private int elementsAppended = 0;
	private int capacity;

	/**
	 * Don't call this constructor except for testing purposes.
	 *
	 * @param size number of elements in the column vector
	 */
	public HeapBytesVector(int size) {
		super(size);
		capacity = size;
		buffer = new byte[capacity];
		start = new int[size];
		length = new int[size];
	}

	@Override
	public void reset() {
		super.reset();
		elementsAppended = 0;
	}

	/**
	 * 通过实际复制到本地缓冲区来设置字段.如果必须将数据实际复制到数组中，请使用此方法.
	 * 不要使用此方法，除非使用setRef()通过引用设置数据是不切实际的.通过引用设置数据往往比复制数据运行得快得多.
	 * Set a field by actually copying in to a local buffer.
	 * If you must actually copy data in to the array, use this method.
	 * DO NOT USE this method unless it's not practical to set data by reference with setRef().
	 * Setting data by reference tends to run a lot faster than copying data in.
	 *
	 * @param elementNum index within column vector to set
	 * @param sourceBuf  container of source data
	 * @param start      start byte position within source
	 * @param length     length of source byte sequence
	 */
	public void setVal(int elementNum, byte[] sourceBuf, int start, int length) {
		reserve(elementsAppended + length);
		System.arraycopy(sourceBuf, start, buffer, elementsAppended, length);
		this.start[elementNum] = elementsAppended;
		this.length[elementNum] = length;
		elementsAppended += length;
	}

	/**
	 * Set a field by actually copying in to a local buffer.
	 * If you must actually copy data in to the array, use this method.
	 * DO NOT USE this method unless it's not practical to set data by reference with setRef().
	 * Setting data by reference tends to run a lot faster than copying data in.
	 *
	 * @param elementNum index within column vector to set
	 * @param sourceBuf  container of source data
	 */
	public void setVal(int elementNum, byte[] sourceBuf) {
		setVal(elementNum, sourceBuf, 0, sourceBuf.length);
	}

	// 确保容量可用，不够则扩为需要扩容的2倍
	private void reserve(int requiredCapacity) {
		if (requiredCapacity > capacity) {
			int newCapacity = requiredCapacity * 2;
				try {
					byte[] newData = new byte[newCapacity];
					// 原有数据拷贝到新数组中
					System.arraycopy(buffer, 0, newData, 0, elementsAppended);
					buffer = newData;
					capacity = newCapacity;
				} catch (OutOfMemoryError outOfMemoryError) {
					throw new UnsupportedOperationException(requiredCapacity + " cannot be satisfied.", outOfMemoryError);
				}
		}
	}

	@Override
	public Bytes getBytes(int i) {
		if (dictionary == null) {
			return new Bytes(buffer, start[i], length[i]);
		} else {
			byte[] bytes = dictionary.decodeToBinary(dictionaryIds.vector[i]);
			return new Bytes(bytes, 0, bytes.length);
		}
	}
}
