package de.komoot.hackathon.pactPlan;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

import static de.komoot.hackathon.pactPlan.NodesInAreas.*;

public class IntersectMatcher extends MatchStub {

	private PactGeometry reusableLeftGeometry;
	private PactGeometry reusableAreaGeometry;
	private PactString leftID;
	private PactString areaID;
	private PactString optName;

	int matchAttempts = 0;
	int successfulMatches = 0;

	public IntersectMatcher() {
		this.reusableLeftGeometry = new PactGeometry();
		this.reusableAreaGeometry = new PactGeometry();
		this.leftID = new PactString();
		this.areaID = new PactString();
		this.optName = new PactString();
	}

	@Override
	public void match(PactRecord leftCell, PactRecord cellWithArea,
			Collector<PactRecord> out) throws Exception {
		matchAttempts++;

		PactGeometry leftGeometry = leftCell.getField(GEO_OBJECT_COLUMN,
				this.reusableLeftGeometry);
		PactGeometry areaGeometry = cellWithArea.getField(GEO_OBJECT_COLUMN,
				this.reusableAreaGeometry);

		if (leftGeometry.getGeo().intersects(areaGeometry.getGeo())) {
			PactRecord outRecord = new PactRecord(3);
			outRecord.setField(ID_COLUMN, leftCell.getField(ID_COLUMN, this.leftID));
			outRecord.setField(OPT_NAME_COLUMN, leftCell.getField(OPT_NAME_COLUMN, this.optName));
			outRecord.setField(AREA_ID_COLUMN, cellWithArea.getField(ID_COLUMN, this.areaID));
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
