package de.komoot.hackathon.pactPlan;

import java.util.List;

import org.apache.log4j.Logger;

import de.komoot.hackathon.Grid;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

import static de.komoot.hackathon.pactPlan.NodesInAreas.*;

public class Gridify extends MapStub {

	private static final Logger LOG = Logger.getLogger(Gridify.class);

	private Grid grid;

	private PactEnvelope reusableEnvelope;

	String type = null;

	int totalInputTuples = 0;

	int totalOutputTuples = 0;

	public Gridify() {
		reusableEnvelope = new PactEnvelope();
	}

	@Override
	public void map(PactRecord record, Collector<PactRecord> out)
			throws Exception {

		totalInputTuples++;
		if (type == null) {
			String geoID = record.getField(NodesInAreas.ID_COLUMN,
					PactString.class).toString();
			if (geoID.startsWith("A")) {
				type = "Area";
			} else if (geoID.startsWith("N")) {
				type = "Node";
			} else if (geoID.startsWith("W")) {
				type = "Way";
			}
		}

		PactEnvelope env = record.getField(ENVELOPE_COLUMN, reusableEnvelope);

		List<String> ids = grid.getIdsForGeometry(env.getEnvelope());

		for (String id : ids) {
			PactRecord outputRecord = new PactRecord();
			outputRecord.addField(record.getField(ID_COLUMN, PactString.class));
			outputRecord.addField(record.getField(OPT_NAME_COLUMN,
					PactString.class));
			outputRecord.addField(record.getField(GEO_OBJECT_COLUMN,
					PactGeometry.class));
			outputRecord.addField(new PactString(id));
			totalOutputTuples++;
			out.collect(outputRecord);
		}
	}

	public void close() {

		if (type == null) {
			type = "unknown";
		}
		LOG.info(String.format(
				"gridify type: %s | input tuples: %d | output tuples: %d",
				type, totalInputTuples, totalOutputTuples));
	}

	public void open(Configuration config) {
		double cellWidth = Double.parseDouble(config.getString(
				NodesInAreas.GRIDIFY_CELL_WIDTH, "0.01"));
		LOG.info("Creating grid with cell width " + cellWidth);
		this.grid = new Grid(cellWidth);
	}
}
