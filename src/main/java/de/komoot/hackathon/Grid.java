package de.komoot.hackathon;

import java.util.LinkedList;
import java.util.List;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Simple Regular grid implementation
 * 
 * @author jan
 */
public class Grid {

	private double cellWidth;

	private int cellsPerGridLine;
	private int cellsPerGridColumn;

	/**
	 * Creates a new grid for WGS84 coordinates with the given cell width
	 * 
	 * @param cellWidth
	 *            width of a cell in degrees
	 */
	public Grid(double cellWidth) {
		this.cellsPerGridColumn = (int) (180.0 / cellWidth);
		
		if ((180.0 / cellWidth) != this.cellsPerGridColumn) {
			throw new IllegalArgumentException(
					"Grid width/height is not a multiple cell width!");
		}

		this.cellsPerGridLine = (int) (360.0 / cellWidth);
		
		this.cellWidth = cellWidth;
	}

	public List<String> getIdsForGeometry(Geometry g) {
		Envelope ge = g.getEnvelopeInternal();
		return getIdsForGeometry(ge);
	}

	public List<String> getIdsForGeometry(Envelope ge) {
		LinkedList<String> ids = new LinkedList<String>();

		Cell upperLeftCell = toCell(ge.getMinX(), ge.getMinY());
		Cell lowerRightCell = toCell(ge.getMaxX(), ge.getMaxY());

		int nextCellY = upperLeftCell.y;
		while (nextCellY <= lowerRightCell.y) {
			int nextCellX = upperLeftCell.x;

			while (nextCellX <= lowerRightCell.x) {
				Cell nextCell = new Cell(nextCellX, nextCellY);
				ids.add(nextCell.toString());
				nextCellX++;
			}
			nextCellY++;
		}

		return ids;
	}

	private Cell toCell(double wgs84XCoordinate, double wgs84YCoordinate) {
		int cellX = ((int) (Math.abs(-180.0 - wgs84XCoordinate) / cellWidth)) % cellsPerGridLine;
		int cellY = ((int) (Math.abs(-90.0 - wgs84YCoordinate) / cellWidth)) % cellsPerGridColumn;

		return new Cell(cellX, cellY);
	}

	private class Cell {
		int x;
		int y;

		public Cell(int x, int y) {
			this.x = x;
			this.y = y;
		}

		public int getCellIndex() {
			return y * cellsPerGridLine + x;
		}

		public String toString() {
			return Integer.toString(getCellIndex());
		}
	}
}
