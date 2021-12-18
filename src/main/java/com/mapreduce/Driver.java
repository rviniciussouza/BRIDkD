package com.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.helpers.ReaderLocalFileSystem;
import com.mapreduce.grouping.PartitionDistanceGroupingComparator;
// import com.mapreduce.mapper.BasedPivotPartitionMapper;
import com.mapreduce.mapper.BasedPivotPartitionOverleapMapper;
// import com.mapreduce.mapper.BasedPivotPartitionMapper;
// import com.mapreduce.mapper.BasedPivotPartitionOverleapMapper;
import com.mapreduce.mapper.ForwardPartialResultsMapper;
// import com.mapreduce.mapper.RandomPartitionMapper;
import com.mapreduce.partitioner.PartitionDistancePartitioner;
import com.mapreduce.reducer.BridIntermediaryReducer;
import com.mapreduce.reducer.BridReducer;
import com.types.PartitionDistancePair;
import com.types.TupleWritable;

public class Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		ReaderLocalFileSystem read = new ReaderLocalFileSystem(args[3]);
		System.out.println("MÉTODO DE PARTICIONAMENTO: BASEADO EM PIVÔ COM SOBREPOSIÇÃO");
		JobControl jobControl = new JobControl("experiment");
		for(int consulta = 0; read.hasNextLine(); consulta++) {
			String query = read.nextLine();

			/* Configurações do JOB 1 */
			Configuration confOfPartitioner = getConf();			
			confOfPartitioner.set("brid.query", query);
			Job jobPartitioner = Job.getInstance(confOfPartitioner, "FIRST JOB");
			jobPartitioner.setJarByClass(Driver.class);
			jobPartitioner.setMapperClass(BasedPivotPartitionOverleapMapper.class);
			jobPartitioner.setReducerClass(BridIntermediaryReducer.class);
			jobPartitioner.setPartitionerClass(PartitionDistancePartitioner.class);
			jobPartitioner.setGroupingComparatorClass(PartitionDistanceGroupingComparator.class);
			jobPartitioner.setMapOutputKeyClass(PartitionDistancePair.class);
			jobPartitioner.setMapOutputValueClass(TupleWritable.class);
			jobPartitioner.setOutputKeyClass(NullWritable.class);
			jobPartitioner.setOutputValueClass(Text.class);
			jobPartitioner.setNumReduceTasks(confOfPartitioner.getInt("mapper.number.partitions", 1));
			FileInputFormat.addInputPath(jobPartitioner, new Path(args[0]));
			FileOutputFormat.setOutputPath(jobPartitioner, new Path(args[1] + consulta + "/"));
			/** Controlled Job */
			ControlledJob controlledJobPartitioner = new ControlledJob(confOfPartitioner);
    		controlledJobPartitioner.setJob(jobPartitioner);
			jobControl.addJob(controlledJobPartitioner);

			/* Configurações do JOB 2 */
			Configuration confOfRefinement = getConf();
			confOfRefinement.set("brid.query", query);
			Job jobRefinement = Job.getInstance(confOfRefinement, "SECOND JOB");
			jobRefinement.setJarByClass(Driver.class);
			jobRefinement.setMapperClass(ForwardPartialResultsMapper.class);
			jobRefinement.setReducerClass(BridReducer.class);
			jobRefinement.setMapOutputKeyClass(PartitionDistancePair.class);
			jobRefinement.setMapOutputValueClass(TupleWritable.class);
			jobRefinement.setPartitionerClass(PartitionDistancePartitioner.class);
			jobRefinement.setGroupingComparatorClass(PartitionDistanceGroupingComparator.class);
			jobRefinement.setOutputKeyClass(NullWritable.class);
			jobRefinement.setOutputValueClass(Text.class);
			jobRefinement.setNumReduceTasks(1);
			FileInputFormat.addInputPath(jobRefinement, new Path(args[1] + consulta));
			FileOutputFormat.setOutputPath(jobRefinement, new Path(args[2] + consulta));
			/** Controlled Job and dependecy */
			ControlledJob controlledJobRefinement = new ControlledJob(confOfRefinement);
    		controlledJobRefinement.setJob(jobRefinement);
			controlledJobRefinement.addDependingJob(controlledJobPartitioner);
			jobControl.addJob(controlledJobRefinement);
		}

		Thread jobControlThread = new Thread(jobControl);
    	jobControlThread.start();
		while (!jobControl.allFinished()) {
			System.out.println("JOB STATUS:");
			System.out.println("\tJobs in waiting state: " + jobControl.getWaitingJobList().size());  
			System.out.println("\tJobs in ready state: " + jobControl.getReadyJobsList().size());
			System.out.println("\tJobs in running state: " + jobControl.getRunningJobList().size());
			System.out.println("\tJobs in success state: " + jobControl.getSuccessfulJobList().size());
			System.out.println("\tJobs in failed state: " + jobControl.getFailedJobList().size());
			Thread.sleep(5000);
		}
		return jobControl.getFailedJobList().size() > 0 ? 1 : 0;
	}

}