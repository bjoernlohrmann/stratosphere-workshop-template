package de.komoot.hackathon.pactPlan;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;

public class NodesReducer extends ReduceStub {

	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

}
