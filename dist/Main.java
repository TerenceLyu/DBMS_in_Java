/**
 * TerenceLyu
 * blu96@brandeis.edu
 * cs127_pa3
 * 2019/3/28
 */
import java.io.*;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import IO_Util.BufferedDataInputStream;
import IO_Util.BufferedDataOutputStream;
public class Main
{
	//data/xxxs/A.csv,data/xxxs/B.csv,data/xxxs/C.csv,data/xxxs/D.csv,data/xxxs/E.csv
	//1
	//SELECT SUM(D.c0), SUM(D.c4), SUM(C.c1)
	//FROM A, B, C, D
	//WHERE A.c1 = B.c0 AND A.c3 = D.c0 AND C.c2 = D.c2
	//AND D.c3 = -9496;
	//SELECT SUM(A.c1)
	//FROM A, C, D
	//WHERE A.c2 = C.c0 AND A.c3 = D.c0 AND C.c2 = D.c2
	//AND C.c2 = 2247;
//	public static Queue<String> fileToDelete = new LinkedList<>();
	public static HashMap<String, Integer> unique = new HashMap<>();
	public static void main(String[] args) throws IOException
	{
		new File("out").mkdirs();
		HashMap<Character, Relation> relations = handle_Data_Loading();
//		System.out.println("data loaded");
		Scanner input = new Scanner(System.in);
		int numberOfQueries = input.nextInt();
//		for (int i = 0; i < 14; i++)
//		{
//			input.nextLine();
//			input.nextLine();
//			input.nextLine();
//			input.nextLine();
//			input.nextLine();
//		}
		
//		Instant start = Instant.now();
		for (int i = 0; i < numberOfQueries; i++)
		{
			input.nextLine();//skip empty line
//			System.out.println("select: ");
			String select = input.nextLine().split(" ", 2)[1];
//			System.out.println("from: ");
			String from = input.nextLine().split(" ", 2)[1];
//			System.out.println("where: ");
			String where = input.nextLine().split(" ", 2)[1];
//			System.out.println("and: ");
			String and = input.nextLine().split(" ", 2)[1];
			String[] scans = and.substring(0,and.length()-1).split(" AND ");
//			System.out.println(1 + select);
//			System.out.println(2 + from);
//			System.out.println(3 + where);
//			System.out.println(4 + and);
			
			
			String[] tableNames = from.split(", ");
			ArrayList<String> usefulCol = new ArrayList<>();
			String temp = select.substring(4, select.length()-1);
			usefulCol.addAll(Arrays.asList(temp.split("[)], SUM[(]")));
			usefulCol.addAll(Arrays.asList(where.split(" = | AND ")));
			temp = and.substring(0, and.length()-1);
			String[] x = temp.split(" AND ");
			for (String y : x)
			{
				usefulCol.add(y.split(" ", 2)[0]);
			}
			HashMap<Character, Table> tables = composeTable(usefulCol, tableNames, relations);
			Table scanned = new Table("X", 0, 0);
			for (int j = 0; j < scans.length; j++)
			{
				for (int k = 0; k < tableNames.length; k++)
				{
					if (tableNames[k].charAt(0) == scans[j].charAt(0)){
						scanned = tableScan(tables.get(scans[j].charAt(0)), scans[j]);
						tables.put(scans[j].charAt(0), scanned);
					}
				}
			}
//			System.out.println("table scaned");
			//first join
			String[] joins = where.split(" AND ");
			ArrayList<String> joinArray = new ArrayList<>(Arrays.asList(joins));
			Table[] firstJoin = new Table[2];
			String joinToRemove = "";
			double minCost = -1;
			for (Table t1 : tables.values())
			{
				for (Table t2 : tables.values())
				{
					if (!t1.getPath().equals(t2.getPath()))
					{
						for (String join : joinArray)
						{
							String[] js = join.split(" ");
							char nameA = js[0].charAt(0);
							char nameB = js[2].charAt(0);
							if (t1.getPath().indexOf(nameA)>=0 && t2.getPath().indexOf(nameB)>=0)
							{
								double cost = cost(t1, t2, js[0], js[2]);
								if (minCost == -1 || minCost > cost)
								{
									minCost = cost;
									firstJoin[0] = t1;
									firstJoin[1] = t2;
									joinToRemove = join;
								}
							}
							if (t1.getPath().indexOf(nameB)>=0 && t2.getPath().indexOf(nameA)>=0)
							{
								double cost = cost(t1, t2, js[2], js[0]);
								if (minCost == -1 || minCost > cost)
								{
									minCost = cost;
									firstJoin[0] = t2;
									firstJoin[1] = t1;
									joinToRemove = join;
								}
							}
						}
						
					}
				}
			}
			Table joinResult;
			if (firstJoin[0].getRowCount()>firstJoin[1].getRowCount())
			{
				joinResult = join(firstJoin[0], firstJoin[1], joinToRemove);
			}else
			{
				joinResult = join(firstJoin[1], firstJoin[0], joinToRemove);
				
			}
			tables.values().remove(firstJoin[0]);
			tables.values().remove(firstJoin[1]);
			joinArray.remove(joinToRemove);
//			try
//			{
//				TimeUnit.MINUTES.sleep(1);
//			}catch (InterruptedException e){}
			joinResult = leftDeepJoin(joinResult, tables, joinArray);
			
			String[] sums = select.split(", ");
			sum(joinResult, sums);
//			for (String fileName : fileToDelete)
//			{
//				File f = new File(fileName);
//				f.delete();
//			}
//			try
//			{
//				TimeUnit.MINUTES.sleep(1);
//			}catch (Exception e){}
		}
		
		File folder = new File("out");
		String[]entries = folder.list();
		for(String s: entries){
			File currentFile = new File(folder.getPath(),s);
			currentFile.delete();
		}
		folder.delete();

		
//		Instant finish = Instant.now();
//		long timeElapsed = Duration.between(start, finish).toMillis();  //in millis
		
		
//		System.out.println(timeElapsed);
//		File[] files = folder.listFiles();
//		if(files!=null) { //some JVMs return null for empty dirs
//			for(File f: files) {
//				f.delete();
//			}
//		}
//		folder.delete();
//		for (Table t: names.values())
//		{
//			File f = new File(t.getPath());
//			f.delete();
//		}
		
	}
	public static HashMap<Character, Relation> handle_Data_Loading() throws IOException
	{
		Scanner input = new Scanner(System.in);
		String[] filenames = input.nextLine().split(",");
		HashMap<Character, Relation> relations = new HashMap<>();
		int letter = 65;
		for (String filename : filenames)
		{
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line = br.readLine();
			String[] cols = line.split(",");
			int columnCount = cols.length;
			DataOutputStream[] dos = new DataOutputStream[columnCount];
			String[] outName = new String[columnCount];
			ArrayList<HashSet<Integer>> diffNum = new ArrayList<>(columnCount);
			for (int i = 0; i < columnCount; i++)
			{
				diffNum.add(new HashSet<>());
				outName[i] = (char) letter + ".c" + i;
				dos[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("out/" + outName[i])));
			}
			FileReader fr = new FileReader(new File(filename));
			CharBuffer cb1 = CharBuffer.allocate(4 * 1024);
			CharBuffer cb2 = CharBuffer.allocate(4 * 1024);
			
			int rowCount = 0;
			int col = 0;
			while (fr.read(cb1) != -1)
			{
				cb1.flip();
				int startOfNumber = 0;
				for (int i = 0; i < cb1.length(); i++)
				{
					
					if (cb1.charAt(i) == ',' || cb1.charAt(i) == '\n')
					{
						int intToWrite = Integer.parseInt(cb1, startOfNumber, i, 10);
						diffNum.get(col).add(intToWrite);
						dos[col].writeInt(intToWrite);
						col = (col + 1)%columnCount;
						if (cb1.charAt(i) == '\n')
						{
							rowCount++;
						}
						startOfNumber = i + 1;
					}
				}
				cb2.clear();
				cb2.append(cb1, startOfNumber, cb1.length());
				CharBuffer tmp = cb2;
				cb2 = cb1;
				cb1 = tmp;
			}
			//			int[] numberOfUnique = new int[columnCount];
			//			for (int i = 0; i < columnCount; i++)
			//			{
			//				numberOfUnique[i] = diffNum.get(i).size();
			//			}
			//			System.out.println(Arrays.toString(numberOfUnique));
			br.close();
			fr.close();
			for (int i = 0; i < columnCount; i++)
			{
				unique.put(outName[i], diffNum.get(i).size());
				dos[i].close();
			}
			Relation r = new Relation(outName, rowCount, columnCount);
			//			t.setNumberOfUnique(numberOfUnique);
			relations.put((char) letter, r);
			letter++;
		}
		return relations;
	}
	public static HashMap<Character, Table> composeTable(ArrayList<String> usefulCol, String[] tableNames, HashMap<Character, Relation> r) throws IOException
	{
		HashMap<Character, Table> tableMap = new HashMap<>();
		for (String name : tableNames)
		{
			char n = name.charAt(0);
			Set<String> completeCol = new HashSet<>();
			for (String col : usefulCol)
			{
				if (col.indexOf(n)>=0)
				{
					completeCol.add(col);
				}
			}
			tableMap.put(n, buildTable(n, completeCol, r.get(n).rowCount));
		}
		return tableMap;
	}
	public static Table buildTable(char n, Set<String> cols, int rowCount) throws IOException
	{
		String[] allCol = cols.toArray(String[]::new);
		Arrays.sort(allCol);
		int colCount = allCol.length;
		DataInputStream[] in = new DataInputStream[colCount];
		HashMap<String, Integer> indexMap = new HashMap<>();
		for (int i = 0; i < colCount; i++)
		{
			in[i] = new DataInputStream(new BufferedInputStream(new FileInputStream("out/" + allCol[i])));
			indexMap.put(allCol[i], i);
		}
		Table t = new Table(String.valueOf(n), colCount, rowCount);
		if (rowCount<6000)
		{
			int[][] data = new int[rowCount][colCount];
			for (int i = 0; i < rowCount; i++)
			{
				for (int j = 0; j < colCount; j++)
				{
					data[i][j] = in[j].readInt();
				}
			}
			t.data = data;
		}else
		{
//			FileOutputStream out = new FileOutputStream("out/" + n);
//			FileChannel file = out.getChannel();
//			ByteBuffer buf = file.map(FileChannel.MapMode.READ_WRITE, 0, 4 * rowCount * colCount);
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("out/" + n)));
			for (int i = 0; i < rowCount; i++)
			{
				
				for (int j = 0; j < colCount; j++)
				{
					dos.writeInt(in[j].readInt());
				}
			}
			dos.close();
		}
		for (int i = 0; i < colCount; i++)
		{
			t.numberOfUnique[i] = unique.get(allCol[i]);
			in[i].close();
		}
		t.indexMap = indexMap;
		return t;
	}
	public static Table tableScan(Table t, String predicate) throws IOException
	{
		
		//A.c14 < -8000
		String[] p = predicate.split(" ");
		//A.c14
		//<
		//-8000
//		System.out.println(t.toString());
//		System.out.println(t.indexMap.keySet().toString());
		char compare = p[1].charAt(0);
		int target = Integer.parseInt(p[2]);
		int col = t.indexMap.get(p[0]);
		int rowCount = 0;
		int tRowCount = t.getRowCount();
		int tColCount = t.getColumnCount();
		ArrayList<HashSet<Integer>> diffNum = new ArrayList<>(tColCount);
		for (int i = 0; i < tColCount; i++)
		{
			diffNum.add(new HashSet<>());
		}
		ArrayList<int[]> result = new ArrayList<>();
		Table nt;
		if (t.data != null)
		{
			for (int i = 0; i < tRowCount; i++)
			{
				if (compare == '=')
				{
					if (t.data[i][col] == target)
					{
//					    System.out.println(row[col]);
						for (int j = 0; j < tColCount; j++)
						{
							diffNum.get(j).add(t.data[i][j]);
						}
						result.add(t.data[i]);
						rowCount++;
					}
				}
				if (compare == '>')
				{
					if (t.data[i][col] > target)
					{
//					    System.out.println(row[col]);
						for (int j = 0; j < tColCount; j++)
						{
							diffNum.get(j).add(t.data[i][j]);
						}
						result.add(t.data[i]);
						rowCount++;
					}
				}
				if (compare == '<')
				{
					if (t.data[i][col] < target)
					{
//					    System.out.println(row[col]);
						for (int j = 0; j < tColCount; j++)
						{
							diffNum.get(j).add(t.data[i][j]);
						}
						result.add(t.data[i]);
						rowCount++;
					}
				}
			}
			nt = new Table("scan_" + t.getPath(), tColCount, rowCount);
			nt.data = result.toArray(new int[rowCount][]);
		}else
		{
			DataInputStream in = new DataInputStream(new BufferedInputStream(
					new FileInputStream("out/" + t.getPath())));
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream("out/scan_" + t.getPath())));
			for (int i = 0; i < tRowCount; i++)
			{
				int[] row = new int[tColCount];
				for (int j = 0; j < tColCount; j++)
				{
					row[j] = in.readInt();
				}
				if (compare == '=')
				{
					if (row[col] == target)
					{
						for (int k = 0; k < tColCount; k++)
						{
							diffNum.get(k).add(row[k]);
							dos.writeInt(row[k]);
						}
						rowCount++;
					}
				}
				if (compare == '>')
				{
					if (row[col] > target)
					{
						for (int k = 0; k < tColCount; k++)
						{
							diffNum.get(k).add(row[k]);
							dos.writeInt(row[k]);
						}
						rowCount++;
					}
				}
				if (compare == '<')
				{
					if (row[col] < target)
					{
						for (int k = 0; k < tColCount; k++)
						{
							diffNum.get(k).add(row[k]);
							dos.writeInt(row[k]);
						}
						rowCount++;
					}
				}
			}
			nt = new Table("scan_" + t.getPath(), tColCount, rowCount);
			in.close();
			dos.close();
		}
		for (int i = 0; i < tColCount; i++)
		{
			nt.numberOfUnique[i] = diffNum.get(i).size();
		}
		HashMap<String, Integer> im = new HashMap<>(t.indexMap);
		nt.indexMap = im;
		return nt;
	}
	public static Table filter(Table t, String preducate) throws IOException
	{
		String fileName = "scan_" + t.getPath();
		String[] p = preducate.split(" = ");
		//A.c45
		int rowCount = 0;
		int tRowCount = t.getRowCount();
		int tColCount = t.getColumnCount();
		
		int aCol = t.indexMap.get(p[0]);
		int bCol = t.indexMap.get(p[1]);
		ArrayList<HashSet<Integer>> diffNum = new ArrayList<>(tColCount);
		for (int i = 0; i < tColCount; i++)
		{
			diffNum.add(new HashSet<>());
		}
		ArrayList<int[]> result = new ArrayList<>();
		Table nt;
		if (t.data != null)
		{
			for (int i = 0; i < tRowCount; i++)
			{
				if (t.data[i][aCol] == t.data[i][bCol])
				{
					for (int j = 0; j < tColCount; j++)
					{
						diffNum.get(j).add(t.data[i][j]);
					}
					result.add(t.data[i]);
					rowCount++;
				}
			}
			nt = new Table("scan_" + t.getPath(), tColCount, rowCount);
			nt.data = result.toArray(new int[rowCount][]);
		}else
		{
			DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream("out/" + t.getPath())));
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("out/" + fileName)));
			
			for (int i = 0; i < tRowCount; i++)
			{
				int[] row = new int[tColCount];
				for (int j = 0; j < tColCount; j++)
				{
					row[j] = in.readInt();
				}
				if (row[aCol] == row[bCol])
				{
					for (int j = 0; j < tColCount; j++)
					{
						diffNum.get(j).add(row[j]);
						dos.writeInt(row[j]);
					}
					rowCount++;
				}
			}
			in.close();
			dos.close();
			nt = new Table(fileName, tColCount, rowCount);
		}
		
		for (int i = 0; i < tColCount; i++)
		{
			nt.numberOfUnique[i] = diffNum.get(i).size();
		}
		nt.indexMap = t.indexMap;
//		fileToDelete.add(nt.getPath());
		return nt;
	}
	public static Table leftDeepJoin(Table t, HashMap<Character, Table> tables, ArrayList<String> joinArray) throws IOException
	{
		while (!joinArray.isEmpty())
		{
			//find a table to join with t
			double minCost = -1;
			Table target = new Table();
			String joinToDo = "";
			for (String join : joinArray)
			{
				String[] js = join.split(" ");
				char nameA = js[0].charAt(0);
				char nameB = js[2].charAt(0);
//				System.out.println(t.toString());
//				System.out.println(Arrays.toString(js));
				if (t.getPath().indexOf(nameA)>=0)
				{
					Table temp = tables.get(nameB);
					double cost = cost(t, temp, js[0], js[2]);
					if (minCost == -1 || minCost > cost)
					{
						minCost = cost;
						target = temp;
						joinToDo = join;
					}
				}
				if (t.getPath().indexOf(nameB)>=0)
				{
					Table temp = tables.get(nameA);
//					System.out.println(temp.toString());
					double cost = cost(t, temp, js[2], js[0]);
					if (minCost == -1 || minCost > cost)
					{
						minCost = cost;
						target = temp;
						joinToDo = join;
					}
				}
			}
//			System.out.println(completedJoin);
			joinArray.remove(joinToDo);
			tables.values().remove(target);
			if (t.getRowCount()>target.getRowCount())
			{
				t = join(t, target, joinToDo);
			}else
			{
				t = join(target, t, joinToDo);
			}
			
			//check for double join
			ArrayList<String> joinToRemove = new ArrayList<>();
			for (String join : joinArray)
			{
				String[] js = join.split(" ");
				char nameA = js[0].charAt(0);
				char nameB = js[2].charAt(0);
				if (t.getPath().indexOf(nameA)>=0 && target.getPath().indexOf(nameB)>=0)
				{
//					System.out.println(join);
					t = filter(t, join);
					joinToRemove.add(join);
				}
				if (t.getPath().indexOf(nameB)>=0 && target.getPath().indexOf(nameA)>=0)
				{
//					System.out.println(join);
					t = filter(t, join);
					joinToRemove.add(join);
				}
			}
			joinArray.removeAll(joinToRemove);
		}
		return t;
	}
	public static Table join(Table a, Table b, String predicate) throws IOException
	{
//		System.out.println(a.getPath()+" join "+b.getPath());
//		System.out.println(a.toString());
//		System.out.println(a.data != null);
//		System.out.println(b.toString());
//		System.out.println(b.data != null);
		String resultName = a.getPath() + "_and_" + b.getPath();
		int rowCount = 0;
		String[] p = predicate.split(" = ");
		int aCol, bCol;
		if (a.getPath().indexOf(p[0].charAt(0))>=0)
		{
			aCol = a.indexMap.get(p[0]);
			bCol = b.indexMap.get(p[1]);
		}else
		{
			aCol = a.indexMap.get(p[1]);
			bCol = b.indexMap.get(p[0]);
		}
		int aColCount = a.getColumnCount();
		int aRowCount = a.getRowCount();
		int bColCount = b.getColumnCount();
		int bRowCount = b.getRowCount();
		ArrayList<HashSet<Integer>> diffNum = new ArrayList<>(aColCount+bColCount);
		for (int i = 0; i < aColCount+bColCount; i++)
		{
			diffNum.add(new HashSet<>());
		}
		Table t;
		if (a.data != null && b.data != null)
		{
			ArrayList<int[]> result = new ArrayList<>();
			for (int i = 0; i < aRowCount; i++)
			{
				for (int j = 0; j < bRowCount; j++)
				{
					if (a.data[i][aCol] == b.data[j][bCol])
					{
						for (int l = 0; l < aColCount; l++)
						{
							diffNum.get(l).add(a.data[i][l]);
						}
						for (int l = 0; l < bColCount; l++)
						{
							diffNum.get(aColCount+l).add(b.data[j][l]);
						}
						result.add(concatenate(a.data[i], b.data[j]));
						rowCount++;
					}
				}
			}
			t = new Table(resultName, aColCount+bColCount, rowCount);
			t.data = result.toArray(new int[rowCount][]);
		}else if (b.data != null)
		{
			DataInputStream inA = new DataInputStream(new BufferedInputStream(
					new FileInputStream("out/"+a.getPath())));
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream("out/"+resultName)));
			for (int i = 0; i < aRowCount;)
			{
				//012345678910
				//A.c1 = B.c0
				int aBlockRow = Math.min(aRowCount-i, 10000);
				HashMap<Integer, ArrayList<int[]>> hashBlock = new HashMap<>();
//			    System.out.println(aBlockRow);
				for (int j = 0; j < aBlockRow; j++)
				{
					int[] aRow = new int[aColCount];
					for (int k = 0; k < aColCount; k++)
					{
						aRow[k] = inA.readInt();
					}
					if (!hashBlock.containsKey(aRow[aCol]))
					{
						ArrayList<int[]> intArray = new ArrayList<>();
						intArray.add(aRow);
						hashBlock.put(aRow[aCol], intArray);
					}else
					{
						hashBlock.get(aRow[aCol]).add(aRow);
					}
				}
				i = i + aBlockRow;
				
				for (int j = 0; j < bRowCount; j++)
				{
					ArrayList<int[]> intArray = hashBlock.get(b.data[j][bCol]);
					if (intArray != null)
					{
						for (int[] aRow : intArray)
						{
							for (int l = 0; l < aColCount; l++)
							{
								diffNum.get(l).add(aRow[l]);
								dos.writeInt(aRow[l]);
							}
							for (int l = 0; l < bColCount; l++)
							{
								diffNum.get(aColCount+l).add(b.data[j][l]);
								dos.writeInt(b.data[j][l]);
							}
							rowCount++;
						}
					}
				}
			}
			inA.close();
			dos.close();
			t = new Table(resultName, aColCount+bColCount, rowCount);
		}else if (a.data != null)
		{
			DataInputStream inB = new DataInputStream(new BufferedInputStream(
					new FileInputStream("out/"+b.getPath())));
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream("out/"+resultName)));
			for (int i = 0; i < bRowCount;)
			{
				//012345678910
				//A.c1 = B.c0
				int BlockRow = Math.min(bRowCount-i, 10000);
				HashMap<Integer, ArrayList<int[]>> hashBlock = new HashMap<>();
//			    System.out.println(aBlockRow);
				for (int j = 0; j < BlockRow; j++)
				{
					int[] bRow = new int[bColCount];
					for (int k = 0; k < bColCount; k++)
					{
						bRow[k] = inB.readInt();
					}
					if (!hashBlock.containsKey(bRow[bCol]))
					{
						ArrayList<int[]> intArray = new ArrayList<>();
						intArray.add(bRow);
						hashBlock.put(bRow[bCol], intArray);
					}else
					{
						hashBlock.get(bRow[bCol]).add(bRow);
					}
				}
				i = i + BlockRow;
				
				for (int j = 0; j < aRowCount; j++)
				{
					ArrayList<int[]> intArray = hashBlock.get(a.data[j][aCol]);
					if (intArray != null)
					{
						for (int[] bRow : intArray)
						{
							
							for (int l = 0; l < aColCount; l++)
							{
								diffNum.get(l).add(a.data[j][l]);
								dos.writeInt(a.data[j][l]);
							}
							for (int l = 0; l < bColCount; l++)
							{
								diffNum.get(l).add(aColCount+bRow[l]);
								dos.writeInt(bRow[l]);
							}
							rowCount++;
						}
					}
				}
			}
			inB.close();
			dos.close();
			t = new Table(resultName, aColCount+bColCount, rowCount);
		}else
		{
			DataInputStream inA = new DataInputStream(new BufferedInputStream(
					new FileInputStream("out/"+a.getPath())));
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream("out/"+resultName)));
			
			for (int i = 0; i < aRowCount;)
			{
				//012345678910
				//A.c1 = B.c0
				int aBlockRow = Math.min(aRowCount-i, 10000);
				HashMap<Integer, ArrayList<int[]>> hashBlock = new HashMap<>();
//			    System.out.println(aBlockRow);
				for (int j = 0; j < aBlockRow; j++)
				{
					int[] aRow = new int[aColCount];
					for (int k = 0; k < aColCount; k++)
					{
						aRow[k] = inA.readInt();
					}
					if (!hashBlock.containsKey(aRow[aCol]))
					{
						ArrayList<int[]> intArray = new ArrayList<>();
						intArray.add(aRow);
						hashBlock.put(aRow[aCol], intArray);
					}else
					{
						hashBlock.get(aRow[aCol]).add(aRow);
					}
				}
				i = i + aBlockRow;
				DataInputStream inB = new DataInputStream(new BufferedInputStream(
						new FileInputStream("out/"+b.getPath())));
				
				for (int j = 0; j < bRowCount; j++)
				{
					int[] bRow = new int[bColCount];
					for (int k = 0; k < bColCount; k++)
					{
						bRow[k] = inB.readInt();
					}
					ArrayList<int[]> intArray = hashBlock.get(bRow[bCol]);
					if (intArray != null)
					{
						for (int[] aRow : intArray)
						{
//						    System.out.println(Arrays.toString(aRow));
							for (int l = 0; l < aRow.length; l++)
							{
								diffNum.get(l).add(aRow[l]);
								dos.writeInt(aRow[l]);
							}
							for (int l = 0; l < bRow.length; l++)
							{
								diffNum.get(aColCount+l).add(bRow[l]);
								dos.writeInt(bRow[l]);
							}
							rowCount++;
						}
					}
				}
				inB.close();
			}
			inA.close();
			dos.close();
			t = new Table(resultName, aColCount+bColCount, rowCount);
		}
		for (int i = 0; i < aColCount+bColCount; i++)
		{
			t.numberOfUnique[i] = diffNum.get(i).size();
		}
		HashMap<String, Integer> im = new HashMap<>(a.indexMap);
		HashMap<String, Integer> bm = b.indexMap;
		for (String key : bm.keySet())
		{
			im.put(key, bm.get(key) + aColCount);
		}
		t.indexMap = im;
//		fileToDelete.add(t.getPath());
//		System.out.println(t.toString());
		return t;
	}
	public static void sum(Table t, String[] sums) throws IOException
	{
		long[] result = new long[sums.length];
//		System.out.println(t);
		HashMap<Integer, Integer> sumMap = new HashMap<>();
		int[] sumIndex = new int[sums.length];
		for (int i = 0; i < sums.length; i++)
		{
			String sum = sums[i].substring(4, sums[i].length()-1);
			sumIndex[i] = t.indexMap.get(sum);
			sumMap.put(i, sumIndex[i]);
		}
		Arrays.sort(sumIndex);
		for (int i = 0; i < sums.length; i++)
		{
			sumMap.put(i, Arrays.binarySearch(sumIndex, sumMap.get(i)));
		}
//		System.out.println(in.available()/4);
//		System.out.println(Arrays.toString(sumIndex));
//		System.out.println(t.getColumnCount());
		int tRowCount = t.getRowCount();
		int tColCount = t.getColumnCount();
		if (t.data != null)
		{
			for (int i = 0; i < tRowCount; i++)
			{
				for (int j = 0; j < sumIndex.length; j++)
				{
					result[j] = result[j] + t.data[i][sumIndex[j]];
				}
			}
		}else
		{
			DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream("out/"+t.getPath())));
			for (int i = 0; i < tRowCount; i++)
			{
				for (int j = 0; j < tColCount; j++)
				{
					
					for (int k = 0; k < sumIndex.length; k++)
					{
//					System.out.print((sumIndex[k]-j)*4 + " ");
						in.skipBytes((sumIndex[k]-j)*4);
						j = sumIndex[k] + 1;
						result[k] = result[k] + in.readInt();
					}
//				    System.out.println((tColCount-j)*4 + " ");
					in.skipBytes((tColCount-j)*4);
					j = tColCount;
				}
			}
			in.close();
		}
		
		for (int i = 0; i < result.length; i++)
		{
			if (tRowCount != 0)
			{
				System.out.print(result[sumMap.get(i)]);
			}
			if (i == result.length - 1)
			{
				System.out.println();
			}else
			{
				System.out.print(",");
			}
		}
	}
	public static double cost(Table a, Table b, String as, String bs)
	{
		//012345
		//C.c20
		//D.c21
//		System.out.println(as + " " + bs);
//		System.out.println(a.toString());
//		System.out.println(b.toString());
		int aCol = a.indexMap.get(as);
		int bCol = b.indexMap.get(bs);
		int aUnique = a.numberOfUnique[aCol];
		int bUnique = b.numberOfUnique[bCol];
//		System.out.println(aUnique);
//		System.out.println(bUnique);
		double result = a.getRowCount() * b.getRowCount() * Math.min(aUnique, bUnique);
		result = result / (aUnique * bUnique);
		return result;
//		System.out.println(result);
//		return a.getRowCount() * b.getRowCount();
	}
	public static <T> T concatenate(T a, T b) {
		if (!a.getClass().isArray() || !b.getClass().isArray()) {
			throw new IllegalArgumentException();
		}
		
		Class<?> resCompType;
		Class<?> aCompType = a.getClass().getComponentType();
		Class<?> bCompType = b.getClass().getComponentType();
		
		if (aCompType.isAssignableFrom(bCompType)) {
			resCompType = aCompType;
		} else if (bCompType.isAssignableFrom(aCompType)) {
			resCompType = bCompType;
		} else {
			throw new IllegalArgumentException();
		}
		
		int aLen = Array.getLength(a);
		int bLen = Array.getLength(b);
		
		@SuppressWarnings("unchecked")
		T result = (T) Array.newInstance(resCompType, aLen + bLen);
		System.arraycopy(a, 0, result, 0, aLen);
		System.arraycopy(b, 0, result, aLen, bLen);
		
		return result;
	}
//	public static Table mergeJoin(Table a, Table b, String predicate) throws IOException
//	{
////		System.out.println(a.getPath()+" join "+b.getPath());
////		System.out.println(a.toString());
////		System.out.println(b.toString());
//		if (a.getRowCount() == 0)
//		{
//			return a;
//		}
//		if (b.getRowCount() == 0)
//		{
//			return b;
//		}
//		String[] p = predicate.split(" = ");
//		int aCol, bCol;
//		if (a.getPath().indexOf(p[0].charAt(0))>=0)
//		{
//			aCol = a.indexMap.get(p[0]);
//			bCol = b.indexMap.get(p[1]);
//		}else
//		{
//			aCol = a.indexMap.get(p[1]);
//			bCol = b.indexMap.get(p[0]);
//		}
//		externalSort(a, aCol);
//		externalSort(b, bCol);
////	    System.out.println(predicate);
////	    System.out.println(p[0].charAt(0)+" "+aCol+" "+p[1].charAt(0)+" "+bCol);
//		int aColCount = a.getColumnCount();
//		int aRowCount = a.getRowCount();
//		int bColCount = b.getColumnCount();
//		int bRowCount = b.getRowCount();
//		int[] tempA = new int[aColCount];
//		int[] tempB = new int[bColCount];
//		DataInputStream inA = new DataInputStream(new BufferedInputStream(
//				new FileInputStream("out/"+a.getPath())));
//		DataInputStream inB = new DataInputStream(new BufferedInputStream(
//				new FileInputStream("out/"+b.getPath())));
//		String resultName = a.getPath() + "_and_" + b.getPath();
//		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
//				new FileOutputStream("out/"+resultName)));
//		for (int i = 0; i < aColCount; i++)
//		{
//			tempA[i] = inA.readInt();
//		}
//		for (int i = 0; i < bColCount; i++)
//		{
//			tempB[i] = inB.readInt();
//		}
//		int rowCount = 0;
//		boolean unfinished = true;
//		boolean buildA = true;
//		boolean buildB = true;
//		ArrayList<int[]> al = new ArrayList<>();
//		ArrayList<int[]> bl = new ArrayList<>();
//		while(unfinished)
//		{
//			boolean done = false;
//			//build A set
//			if (buildA)
//			{
//				al = new ArrayList<>();
//				al.add(tempA);
//				done = false;
//				do
//				{
//					if (inA.available()>0)
//					{
//						int[] aRow = new int[aColCount];
//						for (int i = 0; i < aColCount; i++)
//						{
//							aRow[i] = inA.readInt();
//						}
//
//						if (aRow[aCol] == al.get(0)[aCol])
//						{
//							al.add(aRow);
//						}else
//						{
//							tempA = aRow;
//							done = true;
//						}
//					}else {
//						tempA = null;
//						done = true;
//					}
//
//				}while (!done);
//				buildA = false;
//			}
//			//build B set
//			if (buildB)
//			{
//				bl = new ArrayList<>();
//				bl.add(tempB);
//				done = false;
//				do
//				{
//					if (inB.available()>0)
//					{
//						int[] bRow = new int[bColCount];
//						for (int i = 0; i < bColCount; i++)
//						{
//							bRow[i] = inB.readInt();
//						}
//
//						if (bRow[bCol] == bl.get(0)[bCol])
//						{
//							bl.add(bRow);
//						}else
//						{
//							tempB = bRow;
//							done = true;
//						}
//					}else
//					{
//						tempB = null;
//						done = true;
//					}
//
//				}while (!done);
//				buildB = false;
//			}
////			System.out.println(Arrays.toString(al.get(0)));
////			System.out.println(Arrays.toString(bl.get(0)));
//			if (al.get(0)[aCol] == bl.get(0)[bCol])
//			{
//				for (int i = 0; i < al.size(); i++)
//				{
//					int[] aRow = al.get(i);
////					System.out.println(Arrays.toString(aRow));
//					for (int j = 0; j < bl.size(); j++)
//					{
//						int[] bRow = bl.get(j);
////						System.out.println(Arrays.toString(bRow));
//						for (int k = 0; k < aColCount; k++)
//						{
//							dos.writeInt(aRow[k]);
//						}
//						for (int k = 0; k < bColCount; k++)
//						{
//							dos.writeInt(bRow[k]);
//						}
//						rowCount++;
//					}
//				}
//				buildA = true;
//				buildB = true;
//			}else if (al.get(0)[aCol] > bl.get(0)[bCol])
//			{
//				buildB = true;
//			}else
//			{
//				buildA = true;
//			}
//			if (buildA&&tempA==null)
//			{
//				unfinished = false;
//			}
//			if (buildB&&tempB==null)
//			{
//				unfinished = false;
//			}
//		}
//		inA.close();
//		inB.close();
//		dos.close();
//		Table t = new Table(resultName, aColCount+bColCount, rowCount);
//
//		HashMap<String, Integer> im = new HashMap<>(a.indexMap);
//		HashMap<String, Integer> bm = b.indexMap;
//		for (String key : bm.keySet())
//		{
//			im.put(key, bm.get(key) + aColCount);
//		}
//		t.indexMap = im;
////		fileToDelete.add(t.getPath());
////		System.out.println(t.toString());
//		return t;
//	}
//	public static void externalSort(Table t, int col) throws IOException
//	{
//		DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream("out/" + t.getPath())));
//		int tRowCount = t.getRowCount();
//		int tColCount = t.getColumnCount();
//		int blockCount = 0;
//		//read and sort
//		for (int i = 0; i < tRowCount; i++)
//		{
//			int tBlockRow = Math.min(tRowCount-i, 5000);
//			int[][] tBlock = new int[tBlockRow][tColCount];
//			for (int j = 0; j < tBlockRow; j++)
//			{
//				int[] row = new int[tColCount];
//				for (int k = 0; k < tColCount; k++)
//				{
//					row[k] = in.readInt();
//				}
//				tBlock[j] = row;
//			}
//			i = i + tBlockRow;
//			Arrays.sort(tBlock, (int[] a, int[] b) -> a[col]-b[col]);
//			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
//					new FileOutputStream("out/sort_" + blockCount)));
//			for (int j = 0; j < tBlockRow; j++)
//			{
//				for (int k = 0; k < tColCount; k++)
//				{
//					dos.writeInt(tBlock[j][k]);
//				}
//			}
//			dos.close();
//			blockCount++;
//		}
//		in.close();
//		if (blockCount == 1)
//		{
//			// File (or directory) with old name
//			File file = new File("out/sort_0");
//			File file2 = new File("out/" + t.getPath());
//
//			if (file2.exists())
//			{
//				file2.delete();
//				file2 = new File("out/" + t.getPath());
//			}
//			file.renameTo(file2);
//			return;
//		}
//		mergeFile(tColCount, blockCount, col, t.getPath());
//	}
//	public static void mergeFile(int colCount, int count, int col, String path) throws IOException
//	{
//		DataInputStream[] in = new DataInputStream[count];
//		for (int i = 0; i < count; i++)
//		{
//			in[i] = new DataInputStream(new BufferedInputStream(new FileInputStream("out/sort_" + i)));
//		}
//		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
//				new FileOutputStream("out/" + path)));
//		boolean[] read = new boolean[count];
//		boolean done = false;
//		int min;
//		while (done)
//		{
//			int[] aRow = new int[colCount];
//			int[] bRow = new int[colCount];
//			int[][] block = new int[count][colCount];
//			for (int i = 0; i < count; i++)
//			{
//				for (int j = 0; j < colCount; j++)
//				{
//					if (read[i])
//					{
//						block[i][j] = in[i].readInt();
//						read[i] = false;
//					}
//				}
//			}
//
//			if (aRow[col] == bRow[col])
//			{
//				for (int i = 0; i < colCount; i++)
//				{
//					dos.writeInt(aRow[i]);
//					readA = true;
//				}
//				for (int i = 0; i < colCount; i++)
//				{
//					dos.writeInt(bRow[i]);
//					readB = true;
//				}
//			}else if (aRow[col] < bRow[col])
//			{
//				for (int i = 0; i < colCount; i++)
//				{
//					dos.writeInt(aRow[i]);
//					readA = true;
//				}
//			}else
//			{
//				for (int i = 0; i < colCount; i++)
//				{
//					dos.writeInt(bRow[i]);
//					readB = true;
//				}
//			}
//		}
//		inA.close();
//		inB.close();
//		dos.close();
//	}
//	public static void minOfBlock(int[][] block, int a, int b, int col)
//	{
//		int min = block[0][col];
//		for (int i = 0; i < a; i++)
//		{
//			for (int j = 0; j < b; j++)
//			{
//				if (block[i][j] < min)
//				{
//					min = block[i][j];
//				}
//			}
//		}
//	}
}









