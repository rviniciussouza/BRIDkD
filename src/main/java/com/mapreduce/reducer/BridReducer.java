package com.mapreduce.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import com.algorithms.Brid;
import com.types.Tuple;
import com.types.TupleComparator;
import com.types.TupleWritable;

public class BridReducer extends BaseReducer<IntWritable, TupleWritable, NullWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<TupleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		List<Tuple> dataset = new ArrayList<>();
		for (TupleWritable tuple : values) {
			Tuple copy = new Tuple();
			copy.setId(tuple.getId());
			copy.setDescription(tuple.getDescription());
			copy.setDistance(tuple.getDistance());
			copy.setAttributes(tuple.getAttributes());
			dataset.add(copy);
		}
		Collections.sort(dataset, new TupleComparator(this.query, this.metric));
		Brid brid = new Brid(dataset, this.metric);
		List<Tuple> result = brid.search(this.query, this.K);

		Text attrs = new Text();
		for (Tuple tuple : result) {
			attrs.set(tuple.asString());
			context.write(NullWritable.get(), attrs);
		}
	}
}