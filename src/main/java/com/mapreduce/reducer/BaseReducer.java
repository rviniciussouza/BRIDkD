package com.mapreduce.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import com.helpers.ParserTuple;
import com.metrics.Eucledian;
import com.metrics.Metric;
import com.types.Tuple;

public abstract class BaseReducer<KeyIN, ValueIN, KeyOUT, ValueOUT>
		extends Reducer<KeyIN, ValueIN, KeyOUT, ValueOUT> {

	protected Tuple query;
	protected Metric metric;
	protected Integer K;

	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		ParserTuple parserQuery = new ParserTuple();
		this.query = parserQuery.parse(conf.get("query"));
		this.K = conf.getInt("K", 5);
		this.defineMetric();
	}

	protected void defineMetric() {
		this.metric = new Eucledian();
	}
}