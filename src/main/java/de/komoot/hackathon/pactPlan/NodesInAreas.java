package de.komoot.hackathon.pactPlan;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class NodesInAreas implements PlanAssemblerDescription {

	// schema: GEO_ID, GEO_OBJECT, BOUND_X1, BOUND_X2, BOUND_Y1, BOUND_Y2, CELL_ID 
	public static final int GEO_ID_COLUMN = 0;
	public static final int CELL_ID_COLUMN = 6;
	
	@Override
	public Plan getPlan(String... args) {
		if (args.length != 4) {
			throw new IllegalArgumentException("illegal number of arguments");
		}
		int dop = Integer.valueOf(args[0]);
		String nodesPath = args[1];
		String areasPath = args[2];
		String outputPath = args[3];
		
		FileDataSource nodes = new FileDataSource(GeometryInputFormat.class, nodesPath, "Nodes");
		FileDataSource areas = new FileDataSource(GeometryInputFormat.class, areasPath, "Areas");
		
		MapContract boundNodes = 
				MapContract.builder(BoundingBox.class).input(nodes).name("Add BoundingBox for Nodes").build();
		
		MapContract gridifyNodes = 
				MapContract.builder(Gridify.class).input(boundNodes).name("Gridify Nodes").build();
		
		MapContract boundAreas = 
				MapContract.builder(BoundingBox.class).input(areas).name("Add BoundingBox for Areas").build();
		
		MapContract gridifyAreas = 
				MapContract.builder(Gridify.class).input(boundAreas).name("Gridify Areas").build();
		
		MatchContract matchCells =
				MatchContract.builder(IntersectMatcher.class, PactInteger.class, 
						CELL_ID_COLUMN, CELL_ID_COLUMN)
						.input1(gridifyNodes)
						.input2(gridifyAreas)
						.name("Intersect Nodes and Areas in Matching Cells").build();
		
		ReduceContract reduceNodes = 
				ReduceContract.builder(NodesReducer.class, PactInteger.class, GEO_ID_COLUMN)
						.input(matchCells).name("Reduce by nodeID").build();
		
		FileDataSink output = new FileDataSink(NodesInAreasOutputFormat.class, outputPath, reduceNodes, "Sink");
		
		Plan plan = new Plan(output);
		plan.setDefaultParallelism(dop);
		
		return plan;
	}

	@Override
	public String getDescription() {
		return "dop nodes areas output";
	}

}
