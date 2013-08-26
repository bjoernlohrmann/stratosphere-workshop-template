package de.komoot.hackathon.pactPlan;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.komoot.hackathon.openstreetmap.GeometryModule;
import de.komoot.hackathon.openstreetmap.JsonGeometryEntity;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.wordcount.WordCount.TokenizeLine;

public class GeometryInputFormat extends TextInputFormat
{
	ObjectMapper mapper;
	
	public GeometryInputFormat()
	{
		mapper = new ObjectMapper();
		mapper.registerModule(new GeometryModule());
		mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
	}
	
	public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes)
	{
		boolean result = super.readRecord(target, bytes, offset, numBytes);
		PactString text = target.getField(target.getNumFields()-1, PactString.class);
		String textAsString = text.toString();
		
//		try {
//			JsonGeometryEntity entity = mapper.readValue(textAsString, JsonGeometryEntity.class);
//			entity.getTags().clear();
//		} catch (JsonParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (JsonMappingException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		textAsString = textAsString.substring(textAsString.indexOf(":") + 2);
		String id = textAsString.substring(0, textAsString.indexOf("\""));
		textAsString = textAsString.substring(textAsString.indexOf(":") + 2);
		String geom = textAsString.substring(0, textAsString.indexOf("\""));
		
		target.setField(target.getNumFields()-1, new PactString(id));
		target.addField(new PactString(geom));
		
		return result;
	}
}
