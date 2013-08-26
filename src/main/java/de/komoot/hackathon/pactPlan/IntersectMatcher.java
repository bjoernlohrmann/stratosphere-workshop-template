package de.komoot.hackathon.pactPlan;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class IntersectMatcher extends MatchStub {
	
	private PactGeometry leftGeometry;
	private PactGeometry areaGeometry;
	private PactString leftID;
	private PactString areaID;

	public IntersectMatcher() {
		this.leftGeometry = new PactGeometry();
		this.areaGeometry = new PactGeometry();
		this.leftID = new PactString();
		this.areaID = new PactString();
	}

	@Override
	public void match(PactRecord leftCell, PactRecord cellWithArea,
			Collector<PactRecord> out) throws Exception {

		leftCell.getField(1, this.leftGeometry);
		cellWithArea.getField(1, this.areaGeometry);

		if (this.leftGeometry.getGeo().intersects(this.areaGeometry.getGeo())) {
			PactRecord outRecord = new PactRecord(2);
			outRecord.setField(0, leftCell.getField(0, this.leftID));
			outRecord.setField(1, leftCell.getField(0, this.areaID));
		}
	}
}
