/** 
 * Copyright 2011 Edmund Kohlwey
 *    
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package rhadoop.rmr;

import static junit.framework.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import rhadoop.rmr.FirstLineHeaderRecordReader;

@RunWith(JUnit4.class)
public class TestFirstLineRecordReader {

	@Test
	public void testSingleSplit() throws Throwable {
		FirstLineHeaderRecordReader reader = new FirstLineHeaderRecordReader();
		FileSplit split = new FileSplit(new Path(
				TestFirstLineRecordReader.class.getResource("/test-input")
						.toURI().toString()), 0, 11, new String[0]);
		TaskAttemptContext context = new TaskAttemptContext(
				new Configuration(), new TaskAttemptID());
		reader.initialize(split, context);
		String[] strings = { "abc", "def", "ghi" };
		long[] offsets = { 0, 4, 8 };
		for (int i = 0; i < 3; i++) {
			assertTrue(reader.nextKeyValue());
			LongWritable offset = reader.getCurrentKey();
			Text line = reader.getCurrentValue();
			assertEquals(offsets[i], offset.get());
			assertEquals(strings[i], line.toString());
		}
		assertFalse(reader.nextKeyValue());
	}

	@Test
	public void testSecondSplitInMiddle() throws Throwable {
		FirstLineHeaderRecordReader reader = new FirstLineHeaderRecordReader();
		FileSplit split = new FileSplit(new Path(
				TestFirstLineRecordReader.class.getResource("/test-input")
						.toURI().toString()), 6, 5, new String[0]);
		TaskAttemptContext context = new TaskAttemptContext(
				new Configuration(), new TaskAttemptID());
		reader.initialize(split, context);
		String[] strings = { "abc", "ghi" };
		long[] offsets = { 0, 8 };
		for (int i = 0; i < 2; i++) {
			assertTrue(reader.nextKeyValue());
			LongWritable offset = reader.getCurrentKey();
			Text line = reader.getCurrentValue();
			assertEquals(offsets[i], offset.get());
			assertEquals(strings[i], line.toString());
		}
		assertFalse(reader.nextKeyValue());
	}

	@Test
	public void testSplitOnFirstLineDelimiter() throws Throwable {
		FirstLineHeaderRecordReader reader = new FirstLineHeaderRecordReader();
		FileSplit split = new FileSplit(new Path(
				TestFirstLineRecordReader.class.getResource("/test-input")
						.toURI().toString()), 4, 7, new String[0]);
		TaskAttemptContext context = new TaskAttemptContext(
				new Configuration(), new TaskAttemptID());
		reader.initialize(split, context);
		String[] strings = { "abc", "def", "ghi" };
		long[] offsets = { 0, 4, 8 };
		for (int i = 0; i < 3; i++) {
			assertTrue(reader.nextKeyValue());
			LongWritable offset = reader.getCurrentKey();
			Text line = reader.getCurrentValue();
			assertEquals(offsets[i], offset.get());
			assertEquals(strings[i], line.toString());
		}
		assertFalse(reader.nextKeyValue());
	}
	
	@Test
	public void testSplitOnSecondLineDelimiter() throws Throwable {
		FirstLineHeaderRecordReader reader = new FirstLineHeaderRecordReader();
		FileSplit split = new FileSplit(new Path(
				TestFirstLineRecordReader.class.getResource("/test-input")
						.toURI().toString()), 8, 3, new String[0]);
		TaskAttemptContext context = new TaskAttemptContext(
				new Configuration(), new TaskAttemptID());
		reader.initialize(split, context);
		String[] strings = { "abc", "ghi" };
		long[] offsets = { 0, 8 };
		for (int i = 0; i < 2; i++) {
			assertTrue(reader.nextKeyValue());
			LongWritable offset = reader.getCurrentKey();
			Text line = reader.getCurrentValue();
			assertEquals(offsets[i], offset.get());
			assertEquals(strings[i], line.toString());
		}
		assertFalse(reader.nextKeyValue());
	}
}
