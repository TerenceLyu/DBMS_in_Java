import java.util.HashMap;

/**
 * TerenceLyu
 * blu96@brandeis.edu
 * cs127_pa3
 * 2019/3/28
 */
public class Table
{
	private String path;
	private int columnCount;
	private long rowCount;
	public HashMap<String, Integer> indexMap = new HashMap<>();
	public int[] numberOfUnique;
	public int[] max;
	public int[] min;
	public int[][] data;
	public Table()
	{
	
	}
	public Table(String path, int columnCount, long rowCount)
	{
		this.path = path;
		this.columnCount = columnCount;
		this.rowCount = rowCount;
		numberOfUnique = new int[columnCount];
	}
	
	@Override
	public String toString()
	{
		return "Table{" +
				"path='" + path + '\'' +
				", columnCount=" + columnCount +
				", rowCount=" + rowCount +
				'}';
	}
	
	public String getPath()
	{
		return path;
	}
	
	public int getColumnCount()
	{
		return columnCount;
	}
	
	public long getRowCount()
	{
		return rowCount;
	}
	
//	public void setPath(String path)
//	{
//		this.path = path;
//	}
//
//	public void setColumnCount(int columnCount)
//	{
//		this.columnCount = columnCount;
//	}
//
//	public void setRowCount(int rowCount)
//	{
//		this.rowCount = rowCount;
//	}
	
//	public void setNumberOfUnique(int[] numberOfUnique)
//	{
//		this.numberOfUnique = numberOfUnique;
//	}
//
//	public int[] getNumberOfUnique()
//	{
//		return numberOfUnique;
//	}
	
//	public int start(char x)
//	{
//		return indexMap.get(x);
//	}
	
//	public void addStart(char x, int y)
//	{
//		indexMap.put(x, y);
//	}
	
//	public void setIndexMap(HashMap<Character, Integer> indexMap)
//	{
//		this.indexMap = indexMap;
//	}
	
//	public HashMap<Character, Integer> getIndexMap()
//	{
//		return indexMap;
//	}
}
