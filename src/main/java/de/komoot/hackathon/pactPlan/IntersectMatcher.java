package de.komoot.hackathon.pactPlan;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class IntersectMatcher extends MatchStub {

	private PactGeometry reusableLeftGeometry;
	private PactGeometry reusableAreaGeometry;
	private PactString leftID;
	private PactString areaID;

	int matchAttempts = 0;
	int successfulMatches = 0;

	public IntersectMatcher() {
		this.reusableLeftGeometry = new PactGeometry();
		this.reusableAreaGeometry = new PactGeometry();
		this.leftID = new PactString();
		this.areaID = new PactString();
	}

	@Override
	public void match(PactRecord leftCell, PactRecord cellWithArea,
			Collector<PactRecord> out) throws Exception {
		matchAttempts++;

		PactGeometry leftGeometry = leftCell.getField(1,
				this.reusableLeftGeometry);
		PactGeometry areaGeometry = cellWithArea.getField(1,
				this.reusableAreaGeometry);

		if (leftGeometry.getGeo().intersects(areaGeometry.getGeo())) {
			PactRecord outRecord = new PactRecord(2);
			outRecord.setField(0, leftCell.getField(0, this.leftID));
			outRecord.setField(1, cellWithArea.getField(0, this.areaID));
			out.collect(outRecord);
			successfulMatches++;
		}
	}

	public void close() {
		System.out
				.println(String
						.format("Match attempts: % d | Successful Matches: %d | Fraction of successful matches: %f",
								matchAttempts, successfulMatches,
								((double) successfulMatches) / matchAttempts));
	}
}
