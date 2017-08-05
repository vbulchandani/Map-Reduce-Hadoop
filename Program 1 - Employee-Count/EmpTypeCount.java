package com.bigData.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* This method is used to counts the different types of employees in the file system provided.
 * 
 * Input to MR is given in the formats.
 * Vishal,Professor
 * Sachin,Singer
 * Patrick,Professor
 * Naveen,Engineer
 * Rick,Navy
 * 
 * Output of the MR will be like in below
 * Professor,2
 * Singer,1
 * Navy,1
 * Engineer,1
 * 
 *  Command to run on Linux 
 * [vbulchandani@localhost ~]$ hadoop jar <Jar File> <Package with Class Name> <Input File> <Output File>

 */
public class EmpTypeCount {

	/**
	 * @param args
	 * @throws IOException pes o
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub

		if(args == null || args.length < 2){
			throw new RuntimeException("Input/Output file path is missing.Please check");
		}
		
		// Create job instance using config object
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJobName("Count the employees in different categories");
		job.setJarByClass(EmpTypeCountDriver.class);
		
		// output should be  <Text,IntWritable>  (Ex : Engineer,1)
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//Static nested mapper class 
		job.setMapperClass(EmpTypeMapper.class);
		
		//Static nested reducer class
		job.setReducerClass(EmpTypeReducer.class);
		 
		//set the location of input and output file path which are given in the terminal  
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		// Once the job completes, exit from the java execution. 
		System.exit(job.waitForCompletion(true) ? 0 : 1 );
		
	}
	
	
	/* 
     Input to the Mapper given in such way
	 * 1,(Vishal,Professor)
	 * 2,(Patrick,Professor)
	 * 3,(Rick,Navy)
	 * 
	  Output of the mapper given in such way
	 * Professor,1
	 * Professor,1
	 * Navy,1
	 * 
	 * 
	 */
	public static class EmpTypeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
		public void map(LongWritable key,Text value,Context ctx) throws IOException, InterruptedException{
			
			String[] tokens = value.toString().split(",");
			
			// we will neglect emp names and take only emp types . Hence token[0] is neglected
			ctx.write(new Text(tokens[1]), new IntWritable(1));
		}
		
	}
	
	/* 
	 Input to the reducer given in such way
	 * Professor,1
	 * Professor,1
	 * Navy,1
	 * 
	 * output of the reducer given in such way
	 * Professor,2               -- sum total of same profession.
	 * Navy,1
	 */
	public static class EmpTypeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		public void reduce(Text key,Iterable<IntWritable> values,Context ctx) throws IOException, InterruptedException{
			
			int sum = 0;
			
			while(values.iterator().hasNext()){
				sum+= values.iterator().next().get();
			}

			ctx.write(key,new IntWritable(sum));
		}
	}

}