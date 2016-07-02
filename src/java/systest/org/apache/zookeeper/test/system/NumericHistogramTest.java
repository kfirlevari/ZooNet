/**
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
package org.apache.zookeeper.test.system;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class NumericHistogramTest {

	@Test
	public void testNumericHistogramInt() {
		NumericHistogram nh = new NumericHistogram(100);
		assertTrue(nh.serialize().equals("100"));
	}

	@Test
	public void testNumericHistogramIntArray() {
		int[] arr = new int[]{100, 3, 2, 4, 5};
		NumericHistogram nh = new NumericHistogram(arr);
		assertTrue(nh.serialize().equals("100-3-2-4-5"));
	}

	@Test
	public void testMerge() {

	}

	@Test
	public void testAdd() {
		NumericHistogram nh = new NumericHistogram(100);
		StringBuilder sb = new StringBuilder();
		sb.append(100);
		sb.append("-");
		for (int i=0; i<100; ++i){
			if (i > 0){
				sb.append("-");
			}
			nh.add(i);
			sb.append(i);
			sb.append("-");
			sb.append(1);
		}
		assertTrue(nh.serialize().equals(sb.toString()));
	}

	@Test
	public void testQuantile() {
	}

	@Test
	public void testSerialize() {
	}

	@Test
	public void testSendableHistogramToIntArr() {
	}

}
