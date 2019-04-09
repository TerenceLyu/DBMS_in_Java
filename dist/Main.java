/**
 * TerenceLyu
 * blu96@brandeis.edu
 * cs127_pa3
 * 2019/3/28
 */
import java.io.*;
import java.nio.CharBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Main
{
	//..\pa3_data\data\xxxs\A.csv,..\pa3_data\data\xxxs\B.csv,..\pa3_data\data\xxxs\C.csv,..\pa3_data\data\xxxs\D.csv,..\pa3_data\data\xxxs\E.csv
	//1
	//SELECT SUM(D.c0), SUM(D.c4), SUM(C.c1)
	//FROM A, B, C, D
	//WHERE A.c1 = B.c0 AND A.c3 = D.c0 AND C.c2 = D.c2
	//AND D.c3 = -9496;
	public static Queue<String> fileToDelete = new LinkedList<>();
	public static void main(String[] args) throws IOException
	{
		HashMap<Character, Table> names = handle_Data_Loading();
		System.out.println("data loaded");
		Scanner input = new Scanner(System.in);
		int numberOfQueries = input.nextInt();
		for (int i = 0; i < numberOfQueries; i++)
		{
			input.nextLine();//skip empty line
			String select = input.nextLine().split(" ", 2)[1];
			String from = input.nextLine().split(" ", 2)[1];
			String where = input.nextLine().split(" ", 2)[1];
			String and = input.nextLine().split(" ", 2)[1];
			String[] scans = and.substring(0,and.length()-1).split(" AND ");
			System.out.println(1 + select);
			System.out.println(2 + from);
			System.out.println(3 + where);
			System.out.println(4 + and);
			String[] tableNames = from.split(", ");
			HashMap<Character, Table> tables = new HashMap<>();
			Table scanned = new Table("X", 0, 0);
			for (int j = 0; j < tableNames.length; j++)
			{
				//enforce the last predicate to reduce table size
				for (int k = 0; k < scans.length; k++)
				{
					if (tableNames[j].charAt(0) == scans[k].charAt(0)){
						System.out.println("start table scaned");
						scanned = tableScan(names.get(scans[k].charAt(0)), scans[k]);
						System.out.println("finish table scaned");
						tables.put(tableNames[j].charAt(0), scanned);
					}else
					{
						tables.put(tableNames[j].charAt(0), names.get(tableNames[j].charAt(0)));
					}
				}
				
				
			}
			System.out.println(scanned.start('D'));
			System.out.println("table scaned");
			String[] joins = where.split(" AND ");
			Table curr = scanned;
			Queue<String> joinQueue = new LinkedList<>();
			for (String j : joins)
			{
				if (j.indexOf(curr.getPath().charAt(5)) > 0)
				{
					joinQueue.add(j);
				}
			}
			for (String j : joins)
			{
				if (!joinQueue.contains(j))
				{
					joinQueue.add(j);
				}
			}
			
			for (String join : joinQueue)
			{
				//A.c3 = D.c0
				String[] jl = join.split(" ");
				char nameA = join.charAt(0);
				char nameB = join.charAt(7);
				if (curr.getPath().indexOf(nameA) > 0)
				{
					Table t = tables.get(nameB);
					if (curr.getPath().indexOf(nameB) > 0)
					{
						curr = filter(curr, join);
					}else
					{
						curr = join(jl[0], jl[2], curr, t);
					}
				}else
				{
					Table t = tables.get(nameA);
					if (curr.getPath().indexOf(nameA) > 0)
					{
						curr = filter(curr, join);
					}else
					{
						curr = join(jl[2], jl[0], curr, t);
					}
				}
				
				System.out.println("table joined");
			}
			
			//sum
			//SUM(D.c0), SUM(D.c4), SUM(C.c1)
			String[] sums = select.split(", ");
			
			DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(curr.getPath())));
			int sum1 = 0;
			int sum2 = 0;
			for (int j = 0; j < curr.getRowCount(); j++)
			{
				for (int k = 0; k < curr.getColumnCount(); k++)
				{
					int x = in.readInt();
					if (k == curr.start('D'))
					{
						sum1 = sum1 + x;
					}
					if (k == curr.start('C') + 1)
					{
						sum2 = sum2 + x;
					}
				}
			}
			System.out.println("****"+sum1+"****");
			System.out.println("****"+sum2+"****");
			
			for (String fileName : fileToDelete)
			{
				File f = new File(fileName);
				f.delete();
			}
			
		}
		for (Table t: names.values())
		{
			File f = new File(t.getPath());
			f.delete();
		}
//		BufferedReader[] tableReader = new BufferedReader[filenames.length];
//		File[] f = new  File[filenames.length];
//		int index = 0;
//		for (String filename : filenames)
//		{
//			f[]
//			tableReader[index] = new BufferedReader(new FileReader(new File(filename)));
//		}
		
	}
	public static HashMap<Character, Table> handle_Data_Loading() throws IOException
	{
		Scanner input = new Scanner(System.in);
		String[] filenames = input.nextLine().split(",");
		HashMap<Character, Table> tables = new HashMap<>();
		int letter = 65;
		for (String filename : filenames)
		{
			FileReader fr = new FileReader(new File(filename));
			FileOutputStream fos = new FileOutputStream(String.valueOf((char) letter));
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fos));
			CharBuffer cb1 = CharBuffer.allocate(4 * 1024);
			CharBuffer cb2 = CharBuffer.allocate(4 * 1024);
			int temp = 1;
			int columnCount = 0;
			int rowCount = 0;
			while (fr.read(cb1) != -1)
			{
				cb1.flip();
				int startOfNumber = 0;
				for (int i = 0; i < cb1.length(); i++)
				{
					if (cb1.charAt(i) == ',' || cb1.charAt(i) == '\n')
					{
						if (columnCount == 0)
						{
							if (cb1.charAt(i) == ',')
							{
								temp++;
							}else
							{
								columnCount = temp;
							}
						}
						if (cb1.charAt(i) == '\n')
						{
							rowCount++;
						}
						int intToWrite = Integer.parseInt(cb1, startOfNumber, i, 10);
						dos.writeInt(intToWrite);
						startOfNumber = i + 1;
					}
				}
				cb2.clear();
				cb2.append(cb1, startOfNumber, cb1.length());
				CharBuffer tmp = cb2;
				cb2 = cb1;
				cb1 = tmp;
			}
			fr.close();
			dos.close();
			Table t = new Table(String.valueOf((char) letter), columnCount, rowCount);
			tables.put((char) letter, t);
			letter++;
		}
		return tables;
	}
	public static Table tableScan(Table t, String predicate) throws IOException
	{
		DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(t.getPath())));
		FileOutputStream fos = new FileOutputStream("scan_" + t.getPath());
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fos));
		//A.c14 < -8000
		String[] p = predicate.split(" ");
		//A.c14
		//<
		//-8000
		char compare = p[1].charAt(0);
		int target = Integer.parseInt(p[2]);
		int col = Integer.parseInt(p[0].substring(3));
		int rowCount = 0;
		for (int i = 0; i < t.getRowCount(); i++)
		{
			int[] row = new int[t.getColumnCount()];
			for (int j = 0; j < t.getColumnCount(); j++)
			{
				row[j] = in.readInt();
			}
			if (compare == '=')
			{
				if (row[col] == target)
				{
					for (int j = 0; j < t.getColumnCount(); j++)
					{
						dos.writeInt(row[j]);
					}
					rowCount++;
				}
			}
			if (compare == '>')
			{
				if (row[col] > target)
				{
					for (int j = 0; j < t.getColumnCount(); j++)
					{
						dos.writeInt(row[j]);
					}
					rowCount++;
				}
			}
			if (compare == '<')
			{
				if (row[col] < target)
				{
					for (int j = 0; j < t.getColumnCount(); j++)
					{
						dos.writeInt(row[j]);
					}
					rowCount++;
				}
			}
		}
		in.close();
		dos.close();
		Table nt = new Table("scan_" + t.getPath(), t.getColumnCount(), rowCount);
		nt.setIndexMap(t.getIndexMap());
		System.out.println(nt.start('D'));
		fileToDelete.add(nt.getPath());
		return nt;
	}
	public static Table join(String ac, String bc, Table a, Table b) throws IOException
	{
		DataInputStream inA = new DataInputStream(new BufferedInputStream(new FileInputStream(a.getPath())));
		System.out.println(a.getPath() + "_join_" + b.getPath());
		System.out.println(a.getPath() + " row: " + a.getRowCount() + ", " + a.getPath() + " col: " + a.getColumnCount());
		System.out.println(b.getPath() + " row: " + b.getRowCount() + ", " + b.getPath() + " col: " + b.getColumnCount());
		String resultName = a.getPath() + "_and_" + b.getPath();
		FileOutputStream fos = new FileOutputStream(resultName);
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fos));
		int rowCount = 0;
		for (int i = 0; i < a.getRowCount(); i++)
		{
			//012345678910
			//A.c1 = B.c0
			int[] aRow = new int[a.getColumnCount()];
			int aCol = a.start(ac.charAt(0)) + Character.getNumericValue(ac.charAt(3));
			int bCol = b.start(bc.charAt(0)) + Character.getNumericValue(bc.charAt(3));
			for (int j = 0; j < a.getColumnCount(); j++)
			{
				aRow[j] = inA.readInt();
			}
			DataInputStream inB = new DataInputStream(new BufferedInputStream(new FileInputStream(b.getPath())));
			for (int j = 0; j < b.getRowCount(); j++)
			{
				
				int[] bRow = new int[b.getColumnCount()];
				for (int k = 0; k < b.getColumnCount(); k++)
				{
					bRow[k] = inB.readInt();
				}
				if (aRow[aCol] == bRow[bCol])
				{
					System.out.println(a.getPath()+" : "+aCol+" : "+aRow[aCol] + " , " + b.getPath()+" : "+bCol+" : "+bRow[bCol]);

//					System.out.println("****"+i);
					for (int k = 0; k < a.getColumnCount(); k++)
					{
//						System.out.print(aRow[k] + ",");
						dos.writeInt(aRow[k]);
					}
					for (int k = 0; k < b.getColumnCount(); k++)
					{
//						System.out.print(bRow[k] + ",");
						dos.writeInt(bRow[k]);
					}
//					System.out.println();
					rowCount++;
//					System.out.println(aRow[aCol] + ", " + bRow[bCol]);
//					System.out.println(rowCount);
				}
				
			}
			inB.close();
			
		}
		inA.close();
		dos.close();
		Table t = new Table(resultName, a.getColumnCount()+b.getColumnCount(), rowCount);
		t.setIndexMap(a.getIndexMap());
		HashMap<Character, Integer> m = b.getIndexMap();
		for (char c : m.keySet())
		{
			int newIndex = m.get(c) + a.getColumnCount();
			System.out.println(c + " , " + newIndex);
			t.addStart(c, newIndex);
		}
		fileToDelete.add(t.getPath());
		return t;
	}
	public static Table filter(Table t, String predicate) throws IOException
	{
		DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(t.getPath())));
		FileOutputStream fos = new FileOutputStream("scan_" + t.getPath());
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fos));
		//C.c2 = D.c2
		String[] p = predicate.split(" ");
		String a = p[0];
		String b = p[2];
		int aCol = t.start(a.charAt(0)) + Character.getNumericValue(a.charAt(2));
		int bCol = t.start(b.charAt(0)) + Character.getNumericValue(b.charAt(2));
		int rowCount = 0;
		for (int i = 0; i < t.getRowCount(); i++)
		{
			int[] row = new int[t.getColumnCount()];
			for (int j = 0; j < t.getColumnCount(); j++)
			{
				row[j] = in.readInt();
			}
			if (row[aCol] == row[bCol])
			{
				for (int j = 0; j < t.getColumnCount(); j++)
				{
					System.out.println(row[j]);
					dos.writeInt(row[j]);
				}
				rowCount++;
			}
			
		}
		in.close();
		dos.close();
		t.setPath("scan_" + t.getPath());
		fileToDelete.add(t.getPath());
		return t;
	}
	public static void sum(Table t, String[] sums) throws IOException
	{
		int[] result = new int[sums.length];
		DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(t.getPath())));
		for (int i = 0; i < t.getRowCount(); i++)
		{
			//0123456789
			//SUM(D.c0)
			//SUM(D.c4)
			//SUM(C.c12)
			//
			for (int j = 0; j < t.getColumnCount(); j++)
			{
				int x = in.readInt();
				for (int k = 0; k < sums.length; k++)
				{
					String sum = sums[k];
					if (j == t.start(sum.charAt(4))
							+ Integer.parseInt(sum.substring(7, sum.length()-1)))
					{
						result[k] = result[k] + x;
					}
				}
			}
		}
		for (int i = 0; i < result.length; i++)
		{
			System.out.print(result[i]);
			if (i == result.length - 1)
			{
				System.out.println();
			}else
			{
				System.out.print(",");
			}
		}
		
	}
}









