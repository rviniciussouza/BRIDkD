package com.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import com.algorithms.Brid;
import com.types.TupleComparator;
import com.types.TupleWritable;

public class BridReducer extends BaseReducer<IntWritable, TupleWritable, NullWritable, Text, TupleWritable> {
	
	@Override
	public void reduce(IntWritable key, Iterable<TupleWritable> values, Context context) throws IOException, InterruptedException {
		List<TupleWritable> dataset = new ArrayList<>();
        //TODO Encontra maneira mais elegante para converter Iterable em List		
		for(TupleWritable tuple : values)  {
			TupleWritable T = new TupleWritable();
			T.setDescription(tuple.getDescription());
			T.setId(tuple.getId());
			T.setDistance(tuple.getDistance());
			T.setAttributes(tuple.getAttributes());
			dataset.add(T);
		};
		
		Collections.sort(dataset, new TupleComparator<TupleWritable>(this.query, this.metric));
		
		Brid<TupleWritable> brid = new Brid<TupleWritable>(dataset, this.metric);
		List<TupleWritable> result = brid.search(this.query, this.K);
		
//		Text descricao = new Text();
		Text attrs = new Text();
		for(TupleWritable tuple : result) {
			attrs.set(tuple.attributesAsString());
//			descricao.set(tuple.getDescription());
			context.write(NullWritable.get(), attrs);
		}
	}
}