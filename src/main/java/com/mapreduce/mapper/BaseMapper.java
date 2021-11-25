package com.mapreduce.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import com.helpers.ParserTuple;
import com.metrics.Eucledian;
import com.metrics.Metric;
import com.types.Tuple;

public abstract class BaseMapper<KeyIN, ValueIN, KeyOUT, ValueOUT> extends Mapper<KeyIN, ValueIN, KeyOUT, ValueOUT> {

	protected Tuple query;
	protected ParserTuple parserRecords;
	protected Integer numberPartitions;
	protected Metric metric;

	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		try {
			this.setFormatData(conf.getStrings("header_dataset"));
			ParserTuple parserQuery = new ParserTuple();
			this.query = parserQuery.parse(conf.get("query"));
			this.numberPartitions = conf.getInt("number_partitions", 2);
			this.setMetric();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected void setMetric() {
		this.metric = new Eucledian();
	}

	protected void setFormatData(String[] params) {
		int dimension = Integer.parseInt(params[0]);
		Boolean have_id = Boolean.parseBoolean(params[1]);
		Boolean have_description = Boolean.parseBoolean(params[2]);
		parserRecords = new ParserTuple(dimension, have_id, have_description);
	}
}