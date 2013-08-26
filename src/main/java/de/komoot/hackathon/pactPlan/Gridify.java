package de.komoot.hackathon.pactPlan;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class Gridify extends MapStub{

	@Override
	public void map(PactRecord record, Collector<PactRecord> out)
			throws Exception {
		
		record.addField(new PactString("0"));
		out.collect(record);
	}
}
