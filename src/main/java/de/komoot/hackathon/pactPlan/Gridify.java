package de.komoot.hackathon.pactPlan;

import java.util.List;

import de.komoot.hackathon.Grid;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

import static de.komoot.hackathon.pactPlan.NodesInAreas.*;

public class Gridify extends MapStub {

	private Grid grid;
	private PactEnvelope reusableEnvelope;
	
	public Gridify() {
		grid = new Grid(1.0);
		reusableEnvelope = new PactEnvelope();
	}
	
	@Override
	public void map(PactRecord record, Collector<PactRecord> out)
			throws Exception {
		
		PactEnvelope env = record.getField(ENVELOPE_COLUMN, reusableEnvelope);
		List<String> ids = grid.getIdsForGeometry(env.getEnvelope());
		
		for(String id : ids)
		{
			PactRecord outputRecord = new PactRecord();
			outputRecord.addField(record.getField(ID_COLUMN, PactString.class));
			outputRecord.addField(record.getField(OPT_NAME_COLUMN, PactString.class));
			outputRecord.addField(record.getField(GEO_OBJECT_COLUMN, PactGeometry.class));
			outputRecord.addField(new PactString(id));
			
			out.collect(outputRecord);
		}
	}
}
