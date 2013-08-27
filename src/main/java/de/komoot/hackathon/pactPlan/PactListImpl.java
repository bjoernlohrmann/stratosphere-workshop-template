package de.komoot.hackathon.pactPlan;

import eu.stratosphere.pact.common.type.base.PactList;
import eu.stratosphere.pact.common.type.base.PactString;

public class PactListImpl extends PactList<PactString> {

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (PactString str : this) {
			if (first) {
				first = false; 
			} else {
				sb.append(',');
			}
			sb.append(str);
		}
		return sb.toString();
	}

}
