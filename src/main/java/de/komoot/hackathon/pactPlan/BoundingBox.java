package de.komoot.hackathon.pactPlan;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;

public class BoundingBox extends MapStub {
	private PactGeometry reusablePactGeoObject = new PactGeometry();
	private PactEnvelope pactEnv = new PactEnvelope();
	
	@Override
	public void map(PactRecord record, Collector<PactRecord> out)
			throws Exception {
		PactGeometry pactGeoObject = record.getField(1, reusablePactGeoObject);
		
		pactEnv.setEnvelope(pactGeoObject.getGeo().getEnvelopeInternal());
		record.setField(2, pactEnv);
		out.collect(record);	
	}
}
