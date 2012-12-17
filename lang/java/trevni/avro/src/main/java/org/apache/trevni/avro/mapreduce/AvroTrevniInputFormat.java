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
package org.apache.trevni.avro.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.HadoopInput;

/** An {@link org.apache.hadoop.mapreduce.InputFormat} for Trevni files.
*
* <p>A subset schema to be read may be specified with {@link
* AvroJob#setInputSchema(Schema)}.
*/
public class AvroTrevniInputFormat<T> extends FileInputFormat<AvroWrapper<T>, NullWritable> {

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  protected List<FileStatus> listStatus(JobContext context) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    context.getConfiguration().setBoolean("mapred.input.dir.recursive", true);
    for (FileStatus file : super.listStatus(context))
      if (file.getPath().getName().endsWith(AvroTrevniOutputFormat.EXT))
        result.add(file);
    return result;
  }
  
  @Override
  public RecordReader<AvroWrapper<T>, NullWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new RecordReader<AvroWrapper<T>, NullWritable>() {
      private AvroColumnReader<T> reader ;
      private float rows;
      private long row;
      private AvroWrapper<T> wrapper = new AvroWrapper<T>(null);
        
      public float getProgress() throws IOException { return row / rows; }
  
      public void close() throws IOException { reader.close(); }

      @Override
      public AvroWrapper<T> getCurrentKey() throws IOException,
          InterruptedException {
        return wrapper;
      }

      @Override
      public NullWritable getCurrentValue() throws IOException,
          InterruptedException {
        return NullWritable.get();
      }

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context)
          throws IOException, InterruptedException {
        FileSplit file = (FileSplit)split;
        context.setStatus(file.toString());
        Configuration conf = context.getConfiguration();
        
        AvroColumnReader.Params params =
          new AvroColumnReader.Params(new HadoopInput(file.getPath(), context.getConfiguration()));
        params.setModel(ReflectData.get());
        if (conf.get(AvroJob.INPUT_SCHEMA) != null)
          params.setSchema(AvroJob.getInputSchema(conf));
        
        this.reader = new AvroColumnReader<T>(params);
        this.rows = reader.getRowCount();
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!reader.hasNext())
          return false;
        wrapper.datum(reader.next());
        row++;
        return true;
      }
  
    };
  }

}
