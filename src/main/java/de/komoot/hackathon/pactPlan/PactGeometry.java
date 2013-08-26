package de.komoot.hackathon.pactPlan;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

import eu.stratosphere.pact.common.type.Value;

public class PactGeometry implements Value {

	private Geometry geo;

	private final WKBReader reader = new WKBReader();

	private final WKBWriter writer = new WKBWriter();

	public PactGeometry(Geometry geo) {
		this.geo = geo;
	}

	public PactGeometry() {
	}

	public Geometry getGeo() {
		return geo;
	}

	public void setGeo(Geometry geo) {
		this.geo = geo;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		byte[] bytes = this.writer.write(this.geo);
		out.writeInt(bytes.length);
		out.write(bytes);
	}

	@Override
	public void read(DataInput in) throws IOException {
		byte[] bytes = new byte[in.readInt()];
		in.readFully(bytes);
		try {
			this.geo = this.reader.read(bytes);
		} catch (ParseException e) {
			throw new IOException(e);
		}
	}
}
