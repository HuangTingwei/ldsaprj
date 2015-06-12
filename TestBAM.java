//adapted from Hadoop-BAM's official example and Anastasios Glaros's group's TestBAM.java (link: https://github.com/glrs/LDSA-1000Genome/blob/master/TestBAM.java)

package org.seqdoop.hadoop_bam.examples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import htsjdk.samtools.SAMRecord;

import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.KeyIgnoringBAMOutputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.Cigar;

import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.KeyIgnoringAnySAMOutputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.SAMFormat;

import org.seqdoop.hadoop_bam.FileVirtualSplit;
import org.seqdoop.hadoop_bam.BAMOutputFormat;

/**
 * This program reads a BAM (or SAM) file, filters reads of which the fragment length is larger than 1000, adds individual ID to each read, and writes the output as SAM file.
 *
 * Note that both the input file(s) and the header must be present in HDFS. And replace pom.xml in Hadoop-BAM-7.0.0/examples/ to pom.xml in the code repository 
 *
 * How to compile the program:
 * Put this program in folder Hadoop-BAM-7.0.0/examples/src/main/java/org/seqdoop/hadoop_bam/examples/, and run commond 'sudo mvn clean package' in the folder Hadoop-BAM-7.0.0/examples/
 *
 * How to run the program:
 * In the folder Hadoop-BAM-7.0.0/examples/, run the command: hadoop jar target/*-jar-with-dependencies.jar org.seqdoop.hadoop_bam.examples.TestBAM <header> <input.bam> <output_directory>
 */
public class TestBAM extends Configured implements Tool {
  
  //customize the outputFormat as SAM file without header
  static class MyOutputFormat extends KeyIgnoringAnySAMOutputFormat<NullWritable> {
      public final static String HEADER_FROM_FILE = "TestBAM.header";

      public MyOutputFormat(){
	super(SAMFormat.SAM);
	//do not write the header into the output file
	setWriteHeader(false);
      }	 
      @Override
      public RecordWriter<NullWritable, SAMRecordWritable> getRecordWriter(TaskAttemptContext ctx) throws IOException {
          final Configuration conf = ctx.getConfiguration();
	  readSAMHeaderFrom(new Path(conf.get(HEADER_FROM_FILE)), conf);
		  
          return super.getRecordWriter(ctx);
      }
  }

  //create a job, configure it and submit it. Finally wait for it to complete.
  public int run(String[] args) throws Exception {
      final Configuration conf = getConf();

      
      conf.set(MyOutputFormat.HEADER_FROM_FILE, args[0]);
      final Job job = new Job(conf);

      job.setJarByClass(TestBAM.class);
      job.setMapperClass (TestBAMMapper.class);
      job.setReducerClass(TestBAMReducer.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(SAMRecordWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass (SAMRecordWritable.class);

      job.setInputFormatClass(AnySAMInputFormat.class);
      job.setOutputFormatClass(TestBAM.MyOutputFormat.class);

      org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, new Path(args[1]));

      org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[2]));
      job.submit();

      if (!job.waitForCompletion(true)) {
          System.err.println("sort :: Job failed.");
          return 1;
      }

	  
    return 0;
  }
  
  
  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
        System.out.printf("Usage: hadoop jar <name.jar> %s <header> <input.bam> <output_directory>\n", TestBAM.class.getCanonicalName());
        System.exit(0);
    }

    int res = ToolRunner.run(new Configuration(), new TestBAM(), args);
    System.exit(res);
  }
}

//Mapper
final class TestBAMMapper
        extends org.apache.hadoop.mapreduce.Mapper<LongWritable,SAMRecordWritable, Text,SAMRecordWritable>
{
	String fileName = new String();
	protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException
	{
		fileName = ((FileVirtualSplit) context.getInputSplit()).getPath().getName().toString();
	}
	
	//read a single line of BAM record, check if its fragment length is over 1000. If yes, it add the individual ID to the record and write it to the context
	@Override
	protected void map(
        	LongWritable ignored, SAMRecordWritable wrec,
          	org.apache.hadoop.mapreduce.Mapper<LongWritable,SAMRecordWritable, Text,SAMRecordWritable>.Context ctx)
            throws InterruptedException, IOException{
        	final SAMRecord record = wrec.get();
		if(record.getInferredInsertSize() > 1000 || record.getInferredInsertSize() < -1000){
			record.setReadName(fileName.substring(29,32) + "\t" + fileName.substring(0,7) + "\t" +record.getReadName());
			wrec.set(record);
			ctx.write(new Text(Integer.toString(wrec.get().getInferredInsertSize())), wrec);
		}
    	}
}

//Reducer
final class TestBAMReducer
        extends org.apache.hadoop.mapreduce.Reducer<Text,SAMRecordWritable, Text,SAMRecordWritable> {

    //write the record to the output file
    @Override
    protected void reduce(
            Text key, Iterable<SAMRecordWritable> records,
            org.apache.hadoop.mapreduce.Reducer<Text, SAMRecordWritable, Text, SAMRecordWritable>.Context ctx)
            throws IOException, InterruptedException {

        final Iterator<SAMRecordWritable> it = records.iterator();

        while (it.hasNext()) {
            SAMRecordWritable a = it.next();
            ctx.write(key, a);
        }
    }
}
