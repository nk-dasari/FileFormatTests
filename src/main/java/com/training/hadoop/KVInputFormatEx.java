package com.training.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class KVInputFormatEx {

	public class KVMapper extends Mapper<IntWritable,Text,Text,IntWritable>{
		
		@Override
		public void map(IntWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			IntWritable one = new IntWritable(1);
			String[] words = value.toString().split(",");
			for(String word:words){
				context.write(new Text(word), one);
			}
		}
	}
	
	public class KVReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		@Override
		public void reduce(Text key,Iterable<IntWritable>values,Context context) throws IOException, InterruptedException{
			int sum=0;
			for(IntWritable value:values){
				sum+= value.get();
			}
			context.write(key, new IntWritable(sum));
			
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "AirLine Count");
		job.setJarByClass(KVInputFormatEx.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(KVMapper.class);
		job.setReducerClass(KVReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

}
