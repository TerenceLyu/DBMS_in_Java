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
	
	public Table(String path, int columnCount)
	{
		this.path = path;
		this.columnCount = columnCount;
	}
	
	public String getPath()
	{
		return path;
	}
	
	public int getColumnCount()
	{
		return columnCount;
	}
	
	public void setPath(String path)
	{
		this.path = path;
	}
}
