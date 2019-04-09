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
	private int rowCount;
	private HashMap<Character, Integer> indexMap = new HashMap<>();
	public Table()
	{
	
	}
	public Table(String path, int columnCount, int rowCount)
	{
		this.path = path;
		this.columnCount = columnCount;
		this.rowCount = rowCount;
		indexMap.put(path.charAt(0), 0);
	}
	
	public String getPath()
	{
		return path;
	}
	
	public int getColumnCount()
	{
		return columnCount;
	}
	
	public int getRowCount()
	{
		return rowCount;
	}
	
	public void setPath(String path)
	{
		this.path = path;
	}
	
	public void setColumnCount(int columnCount)
	{
		this.columnCount = columnCount;
	}
	
	public void setRowCount(int rowCount)
	{
		this.rowCount = rowCount;
	}
	
	public int start(char x)
	{
		return indexMap.get(x);
	}
	public void addStart(char x, int y)
	{
		indexMap.put(x, y);
	}
	public void setIndexMap(HashMap<Character, Integer> indexMap)
	{
		this.indexMap = indexMap;
	}
	
	public HashMap<Character, Integer> getIndexMap()
	{
		return indexMap;
	}
}
