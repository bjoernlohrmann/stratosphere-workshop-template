package de.komoot.hackathon.pactPlan;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

import static de.komoot.hackathon.pactPlan.NodesInAreas.*;

public class NodesReducer extends ReduceStub {

	private PactListImpl reusableAreas = new PactListImpl();
	
	@Override
	public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
			throws Exception {
		PactRecord element = null;
		reusableAreas.clear();
		
		while (records.hasNext()) {
			element = records.next();
			reusableAreas.add(element.getField(AREA_ID_COLUMN, PactString.class));						
		}
		
		if(element != null)
		{
			PactRecord result = new PactRecord();
			result.addField(element.getField(ID_COLUMN, PactString.class));
			result.addField(element.getField(OPT_NAME_COLUMN, PactString.class));
			result.addField(reusableAreas);
			out.collect(result);
		}
	}
}
