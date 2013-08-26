package de.komoot.hackathon.pactPlan;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class GeometryInputFormat extends TextInputFormat
{
	
	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes)
	{
		boolean result = super.readRecord(target, bytes, offset, numBytes);
		PactString text = target.getField(target.getNumFields()-1, PactString.class);
		String textAsString = text.toString();

		
		textAsString = textAsString.substring(textAsString.indexOf(":") + 2);
		String id = textAsString.substring(0, textAsString.indexOf("\""));
		textAsString = textAsString.substring(textAsString.indexOf(":") + 2);
		String geom = textAsString.substring(0, textAsString.indexOf("\""));
		
		target.setField(target.getNumFields()-1, new PactString(id));
		target.addField(new PactGeometry(geom));
		
		return result;
	}
}
