package de.komoot.hackathon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.vividsolutions.jts.geom.Envelope;

/**
 * @author jan
 * @date 26.08.13
 */
public class GridTest {

	@Test
	public void testSimple() {
		Grid g = new Grid(180);
		Envelope envelope = new Envelope(-90, 90, -45, 45);

		List<String> cells = g.getIdsForGeometry(envelope);
		assertEquals(2, cells.size());
		assertEquals("0", cells.get(0));
		assertEquals("1", cells.get(1));
	}

	@Test
	public void testConstructor() {
		try {
			new Grid(0.001);
			assertTrue(true);
		} catch (IllegalArgumentException e) {
			assertTrue(false);
		}
	}

	@Test
	public void testConstructorFail() {
		try {
			new Grid(0.007);
			assertTrue(false);
		} catch (IllegalArgumentException e) {
			assertTrue(true);
		}
	}

	@Test
	public void testOneCell() {
		Grid g = new Grid(180);
		Envelope envelope = new Envelope(-90, -45, -45, 45);

		List<String> cells = g.getIdsForGeometry(envelope);
		assertEquals(1, cells.size());
		assertEquals("0", cells.get(0));
	}

	@Test
	public void testFourCells() {
		Grid g = new Grid(60);
		Envelope envelope = new Envelope(30, 90, -40, 20);

		List<String> cells = g.getIdsForGeometry(envelope);
		assertEquals(4, cells.size());
		assertEquals("3", cells.get(0));
		assertEquals("4", cells.get(1));
		assertEquals("9", cells.get(2));
		assertEquals("10", cells.get(3));
	}

	@Test
	public void testCellBorderTop() {
		Grid g = new Grid(60);
		Envelope envelope = new Envelope(30, 150, -90, 89);

		List<String> cells = g.getIdsForGeometry(envelope);
		assertEquals(9, cells.size());
		assertEquals("3", cells.get(0));
		assertEquals("4", cells.get(1));
		assertEquals("5", cells.get(2));
		assertEquals("9", cells.get(3));
		assertEquals("10", cells.get(4));
		assertEquals("11", cells.get(5));
		assertEquals("15", cells.get(6));
		assertEquals("16", cells.get(7));
		assertEquals("17", cells.get(8));
	}

	@Test
	public void testCellBorderLeft() {
		Grid g = new Grid(60);
		Envelope envelope = new Envelope(-180, -1, -60, 60);

		List<String> cells = g.getIdsForGeometry(envelope);
		assertEquals(9, cells.size());
		assertEquals("0", cells.get(0));
		assertEquals("1", cells.get(1));
		assertEquals("2", cells.get(2));
		assertEquals("6", cells.get(3));
		assertEquals("7", cells.get(4));
		assertEquals("8", cells.get(5));
		assertEquals("12", cells.get(6));
		assertEquals("13", cells.get(7));
		assertEquals("14", cells.get(8));
	}
}
