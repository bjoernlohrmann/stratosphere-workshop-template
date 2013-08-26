package de.komoot.hackathon.pactPlan;

import java.util.List;

import com.vividsolutions.jts.geom.GeometryFactory;

import de.komoot.hackathon.Grid;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class Gridify extends MapStub{

	private GeometryFactory geomFactory;
	private Grid grid;
	
	public Gridify() {
		geomFactory = new GeometryFactory();
		grid = new Grid(0.1d);
	}
	
	@Override
	public void map(PactRecord record, Collector<PactRecord> out)
			throws Exception {
		
		PactEnvelope env = record.getField(2, PactEnvelope.class);
		List<String> ids = grid.getIdsForGeometry(geomFactory.toGeometry(env.getEnvelope()));
		
		for(String id : ids)
		{
			PactRecord outputRecord = new PactRecord();
			outputRecord.addField(record.getField(0, PactString.class));
			outputRecord.addField(new PactString(id));
			
			out.collect(outputRecord);
		}
	}
}
