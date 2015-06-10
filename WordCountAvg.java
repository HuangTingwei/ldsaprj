//package org.myorg;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
	private int tlen = 0;
	    
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] split = line.split("\t");
            word.set(split[0]);
	    tlen = Math.abs(Integer.parseInt(split[9]));
            output.collect(word, new IntWritable(tlen));
            
        }
    }
	
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
	    int count = 0;
	    int avg = 0;
            while (values.hasNext()) {
                sum += values.next().get();
		count += 1;
            }
	    avg = sum/count;
            output.collect(key, new IntWritable(avg));
        }
    }
	
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordcount");
	    
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
	    
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
	    
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
	    
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	    
        JobClient.runJob(conf);
    }
}
