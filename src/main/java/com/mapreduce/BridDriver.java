package com.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.helpers.ReaderLocalFileSystem;
import com.mapreduce.grouping.PartitionDistanceGroupingComparator;
import com.mapreduce.mapper.BasedPivotPartitionMapper;
import com.mapreduce.mapper.ForwardPartialResultsMapper;
import com.mapreduce.partitioner.PartitionDistancePartitioner;
import com.mapreduce.reducer.BridIntermediaryReducer;
import com.mapreduce.reducer.BridReducer;
import com.types.PartitionDistancePair;
import com.types.TupleWritable;

public class BridDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BridDriver(), args);
        System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		ReaderLocalFileSystem read = new ReaderLocalFileSystem(args[3]);
		Integer consulta = 0;
		
		while(read.hasNextLine()) {
			String query = read.nextLine();
			/* Configurações do JOB 1 */
			Configuration conf = getConf();
			conf.set("brid.query", query);
			System.out.println("\n");
			System.out.println("INICIANDO FASE DE PARTIONAMENTO");
			System.out.println("CONSULTA " + consulta); 
			System.out.println("\n");

			Job job = Job.getInstance(conf, "Brid_k_phase_one");
			job.setJarByClass(BridDriver.class);
			job.setMapperClass(BasedPivotPartitionMapper.class);
			job.setReducerClass(BridIntermediaryReducer.class);
			job.setPartitionerClass(PartitionDistancePartitioner.class);
			job.setGroupingComparatorClass(PartitionDistanceGroupingComparator.class);
			job.setMapOutputKeyClass(PartitionDistancePair.class);
			job.setMapOutputValueClass(TupleWritable.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(conf.getInt("mapper.number.partitions", 1));

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1] + consulta + "/"));

			if (!job.waitForCompletion(true)) {
				System.exit(1);
			}

			System.out.println("\n");
			System.out.println("INICIANDO FASE DE REFINAMENTO");
			System.out.println("CONSULTA " + consulta); 
			System.out.println("\n");

			/* Configurações do JOB 2 */
			Configuration conf2 = getConf();
			conf2.set("brid.query", query);
			// conf2.set("header_dataset", args[3]);
			// conf2.set("K", args[4]);
			Job job2 = Job.getInstance(conf2, "Brid_k_phase_final");
			job2.setJarByClass(BridDriver.class);
			job2.setMapperClass(ForwardPartialResultsMapper.class);
			job2.setReducerClass(BridReducer.class);
			job2.setMapOutputKeyClass(PartitionDistancePair.class);
			job2.setMapOutputValueClass(TupleWritable.class);
			job2.setPartitionerClass(PartitionDistancePartitioner.class);
			job2.setGroupingComparatorClass(PartitionDistanceGroupingComparator.class);
			job2.setOutputKeyClass(NullWritable.class);
			job2.setOutputValueClass(Text.class);
            job2.setNumReduceTasks(1);
			FileInputFormat.addInputPath(job2, new Path(args[1] + consulta));
			FileOutputFormat.setOutputPath(job2, new Path(args[2] + consulta));

			/* Aguardando o job completar para apagar os arquivos intermediarios */
			if (!job2.waitForCompletion(true)) {
				System.exit(1);
			}
			consulta++;
		}
		return 0;
	}

	public static void deletePathIfExists(Configuration conf, String stepOutputPath) throws IOException {
		Path path = new Path(stepOutputPath);
		org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}
}