package de.komoot.hackathon.pactPlan;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;

import static de.komoot.hackathon.pactPlan.NodesInAreas.*;

public class BoundingBox extends MapStub {
	private PactGeometry reusablePactGeoObject = new PactGeometry();
	private PactEnvelope pactEnv = new PactEnvelope();
	
	@Override
	public void map(PactRecord record, Collector<PactRecord> out)
			throws Exception {
		PactGeometry pactGeoObject = record.getField(GEO_OBJECT_COLUMN, reusablePactGeoObject);
		
		pactEnv.setEnvelope(pactGeoObject.getGeo().getEnvelopeInternal());
		record.setField(ENVELOPE_COLUMN, pactEnv);
		out.collect(record);	
	}
}
