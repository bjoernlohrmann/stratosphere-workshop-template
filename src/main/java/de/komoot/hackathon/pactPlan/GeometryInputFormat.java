package de.komoot.hackathon.pactPlan;


import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;

public class GeometryInputFormat extends TextInputFormat
{

	PactString reusableId = new PactString();
	PactString reusableGeometryStr = new PactString();
	PactString reusableNameStr = new PactString();
	
	@Override
	public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes)
	{

		PactString str = this.theString;
		
		if (this.ascii) {
			str.setValueAscii(bytes, offset, numBytes);
		}
		else {
			ByteBuffer byteWrapper = this.byteWrapper;
			if (bytes != byteWrapper.array()) {
				byteWrapper = ByteBuffer.wrap(bytes, 0, bytes.length);
				this.byteWrapper = byteWrapper;
			}
			byteWrapper.clear();
			byteWrapper.position(offset);
			byteWrapper.limit(offset + numBytes);
				
			try {
				CharBuffer result = this.decoder.decode(byteWrapper);
				str.setValue(result);
			}
			catch (CharacterCodingException e) {
				return false;
			}
		}
		
		
		if (!findStringValue(str, "id", reusableId))
			throw new RuntimeException("Could not parse id attribute");
		if (!findStringValue(str, "geometry", reusableGeometryStr))
			throw new RuntimeException("Could not parse geometry attribute");
		if (!findStringValue(str, "name", reusableNameStr))
			reusableNameStr.setValue("");
		
		target.clear();
		target.addField(reusableId);
		target.addField(reusableNameStr);
		target.addField(new PactGeometry(reusableGeometryStr.toString()));
		return true;
		
	}
	
	public static boolean findStringValue(PactString text, String attribut, PactString target){
		int startPosition = text.find("\""+attribut+"\":\"");
		if(startPosition == -1) {
			return false;
		}
		startPosition += 4 + attribut.length();
		int endPosition = text.find("\"",startPosition);
		if(endPosition == -1){
			return false;
		}
		text.substring(target,startPosition,endPosition);
		return true;
	}
}
