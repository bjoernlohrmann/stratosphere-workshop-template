package de.komoot.hackathon.pactPlan;

import java.util.Iterator;
import java.util.LinkedList;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class NodesReducer extends ReduceStub {

	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception {
		PactRecord element = null;
		LinkedList<PactString> areas = new LinkedList<PactString>();
		while (records.hasNext()) {
			element = records.next();
			
			for(int i = 1; i < element.getNumFields(); i++)
			{
				PactString area = element.getField(i, PactString.class);
				areas.add(area);
			}			
		}
		
		PactRecord result = new PactRecord();
		
		if(element != null)
		{
			result.addField(element.getField(0, PactString.class));
			for(PactString area : areas)
			{
				result.addField(area);
			}
			
			out.collect(result);
		}
	}
}
