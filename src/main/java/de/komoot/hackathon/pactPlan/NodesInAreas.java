package de.komoot.hackathon.pactPlan;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactString;

public class NodesInAreas implements PlanAssemblerDescription {

	// Input/BoundingBox: GEO_ID, OPT_NAME, GEO_OBJECT, ENVELOPE
	public static final int ID_COLUMN = 0;
	public static final int OPT_NAME_COLUMN = 1;
	public static final int GEO_OBJECT_COLUMN = 2;
	public static final int ENVELOPE_COLUMN = 3;
	// Gridify: GEO_ID, OPT_NAME, GEO_OBJECT, CELL_ID
	public static final int CELL_ID_COLUMN = 3;
	// Intersect: NODE_ID, OPT_NAME, AREA_ID
	public static final int AREA_ID_COLUMN = 2;
	// Reduce: NODE_ID, OPT_NAME, AREA_IDS
	public static final int AREA_IDS_COLUMN = 2;
	
	@Override
	public Plan getPlan(String... args) {

		int dop = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String nodesPath = (args.length > 1 ? args[1] : "");
		String areasPath = (args.length > 2 ? args[2] : "");
		String outputPath = (args.length > 3 ? args[3] : "");

		FileDataSource nodes = new FileDataSource(GeometryInputFormat.class,
				nodesPath, "Nodes");
		FileDataSource areas = new FileDataSource(GeometryInputFormat.class,
				areasPath, "Areas");

		MapContract boundNodes = MapContract.builder(BoundingBox.class)
				.input(nodes).name("Add BoundingBox for Nodes").build();

		MapContract gridifyNodes = MapContract.builder(Gridify.class)
				.input(boundNodes).name("Gridify Nodes").build();

		MapContract boundAreas = MapContract.builder(BoundingBox.class)
				.input(areas).name("Add BoundingBox for Areas").build();

		MapContract gridifyAreas = MapContract.builder(Gridify.class)
				.input(boundAreas).name("Gridify Areas").build();

		MatchContract matchCells = MatchContract
				.builder(IntersectMatcher.class, PactString.class,
						CELL_ID_COLUMN, CELL_ID_COLUMN).input1(gridifyNodes)
				.input2(gridifyAreas)
				.name("Intersect Nodes and Areas in Matching Cells").build();

		ReduceContract reduceNodes = ReduceContract
				.builder(NodesReducer.class, PactString.class, ID_COLUMN)
				.input(matchCells).name("Reduce by nodeID").build();

		FileDataSink output = new FileDataSink(NodesInAreasOutputFormat.class,
				outputPath, reduceNodes, "Sink");

		Plan plan = new Plan(output);
		plan.setDefaultParallelism(dop);

		return plan;
	}

	@Override
	public String getDescription() {
		return "dop nodes areas output";
	}

}
