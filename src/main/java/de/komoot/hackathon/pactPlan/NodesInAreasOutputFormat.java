package de.komoot.hackathon.pactPlan;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

import static de.komoot.hackathon.pactPlan.NodesInAreas.*;

public class NodesInAreasOutputFormat extends FileOutputFormat {

	public static final String ENCODING_PARAMETER = "pact.output.delimited.delimiter-encoding";

	public static final String RECORD_DELIMITER_PARAMETER = "pact.output.record.delimiter";
	
	private String recordDelimiter;
	private String charsetName;
	private Writer wrt;

	private PactString reusableNodeId = new PactString();
	private PactString reusableOptName = new PactString();
	private PactListImpl reusableAreas = new PactListImpl();
	
	
	
	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);
		
		this.recordDelimiter = parameters.getString(RECORD_DELIMITER_PARAMETER, "\n");
		this.charsetName = parameters.getString(ENCODING_PARAMETER, null);
	}

	@Override
	public void open(int taskNumber) throws IOException {
		super.open(taskNumber);
		this.wrt = this.charsetName == null ? new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096)) :
			new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096), this.charsetName);

	}

	@Override
	public void close() throws IOException {
		if (wrt != null) {
			this.wrt.close();
		}
		super.close();
	}

	@Override
	public void writeRecord(PactRecord record) throws IOException {
		PactString id = record.getField(ID_COLUMN, reusableNodeId);
		PactString optName = record.getField(OPT_NAME_COLUMN, reusableOptName);
		PactListImpl areas = record.getField(AREA_IDS_COLUMN, reusableAreas);
		
		wrt.append(id);
		wrt.append(',');
		wrt.append('"');
		wrt.append(optName);
		wrt.append('"');
		
		for (PactString area : areas) {
			wrt.append(',');
			wrt.append(area);
		}
		
		wrt.write(recordDelimiter);
	}

}
