package com.mapreduce;
 
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.helpers.ReadDataset;
import com.types.TupleWritable;

public class BridDriver {

	public static void main(String[] args) throws Exception {
		
		ReadDataset read = new ReadDataset(args[5]);
		List<String> queries = read.getLines();
		
		int iterations = queries.size();
		System.out.println(iterations);
		
		for(int i = 0; i < iterations; i++) {
			/* Configurações do JOB 1 */
			Configuration conf = new Configuration();
			conf.set("query", queries.get(i));
			conf.set("input_info", args[3]);
			conf.set("K", args[4]);
			
			Job job = Job.getInstance(conf, "Brid_k_phase_one");
			job.setJarByClass(BridDriver.class);
			job.setMapperClass(BridIntermediaryMapper.class);
			job.setReducerClass(BridIntermediaryReducer.class);
			
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(TupleWritable.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]+i+"/"));
			
			if(!job.waitForCompletion(true)) {
				System.exit(1);
			}
//			job.waitForCompletion(true);
	//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	
			/* Configurações do JOB 2 */
			Configuration conf2 = new Configuration();
			conf2.set("query", queries.get(i));
			conf2.set("input_info", args[3]);
			conf2.set("K", args[4]);
			Job job2 = Job.getInstance(conf2, "Brid_k_phase_final");
			job2.setJarByClass(BridDriver.class);
			job2.setMapperClass(BridMapper.class);
			job2.setReducerClass(BridReducer.class);
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(TupleWritable.class);
			
			job2.setOutputKeyClass(NullWritable.class);
			job2.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job2, new Path(args[1]+i));
			FileOutputFormat.setOutputPath(job2, new Path(args[2]+i));
			
			/* Aguardando o job completar para apagar os arquivos intermediarios */
//			job2.waitForCompletion(true);
//			BridDriver.deletePathIfExists(conf2, args[1]+i);
			
			if(!job2.waitForCompletion(true)) {
				System.exit(1);
			}
			
		}
		System.exit(0);
	}

	public static void deletePathIfExists(Configuration conf, String stepOutputPath) throws IOException {
		  Path path = new Path(stepOutputPath);
		  org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
		  if(fs.exists(path)) {
			  fs.delete(path, true);
		  }
	}
}