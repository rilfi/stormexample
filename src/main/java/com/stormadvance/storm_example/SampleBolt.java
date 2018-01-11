package com.stormadvance.storm_example;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SampleBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;

	public void execute(Tuple input, BasicOutputCollector collector) {
		// fetched the field "site" from input tuple.
		String test = input.getStringByField("c1");
		collector.emit(new Values(input.getString(0), input.getString(1), System.currentTimeMillis()));
		// print the value of field "site" on console.
		//System.out.println("######### Name of input site is : " + test);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("c1","c2","c3"));
	}
}
