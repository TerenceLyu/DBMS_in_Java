/**
 * TerenceLyu
 * blu96@brandeis.edu
 * cs127_pa3
 * 2019/3/28
 */
import java.io.*;
import java.lang.reflect.Array;
import java.nio.CharBuffer;
import java.util.*;
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
	public static HashMap<String, Integer> MAX = new HashMap<>();
	public static HashMap<String, Integer> MIN = new HashMap<>();
	public static void main(String[] args) throws IOException
	{
		new File("out").mkdirs();
		HashMap<Character, Relation> relations = handle_Data_Loading();
//		System.out.println("data loaded");
		Scanner input = new Scanner(System.in);
		int numberOfQueries = input.nextInt();
//		for (int i = 0; i < 3; i++)
//		{
//			input.nextLine();
//			input.nextLine();
//			input.nextLine();
//			input.nextLine();
//			input.nextLine();
//		}
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
			String[] scans = and.substring(0,and.length()-1).split(" AND | and ");
//			System.out.println(1 + select);
//			System.out.println(2 + from);
//			System.out.println(3 + where);
//			System.out.println(4 + and);
			String[] tableNames = from.split(", ");
			ArrayList<String> usefulCol = new ArrayList<>();
			String temp = select.substring(4, select.length()-1);
			usefulCol.addAll(Arrays.asList(temp.split("[)], SUM[(]")));
			usefulCol.addAll(Arrays.asList(where.split(" = | AND | and ")));
			temp = and.substring(0, and.length()-1);
			String[] x = temp.split(" AND | and ");
			for (String y : x)
			{
				usefulCol.add(y.split(" ", 2)[0]);
			}
			HashMap<Character, Table> tables = composeTable(usefulCol, tableNames, relations, scans);
//			Table scanned = new Table("X", 0, 0);
//			HashMap<String, ArrayList<String>> tableScan= new HashMap<>();
//			for (int j = 0; j < scans.length; j++)
//			{
//				for (int k = 0; k < tableNames.length; k++)
//				{
//					if (tableNames[k].charAt(0) == scans[j].charAt(0)){
//						if (!tableScan.containsKey(tableNames[k]))
//						{
//							ArrayList<String> scanList = new ArrayList<>();
//							scanList.add(scans[j]);
//							tableScan.put(tableNames[k], scanList);
//						}else
//						{
//							tableScan.get(tableNames[k]).add(scans[j]);
//						}
//					}
//				}
//			}
//			for (String tableName : tableScan.keySet())
//			{
//				scanned = tableScan(tables.get(tableName.charAt(0)), tableScan.get(tableName));
//				tables.put(tableName.charAt(0), scanned);
//			}
//			System.out.println("table scaned");
			//first join
			String[] joins = where.split(" AND | and ");
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
		Arrays.sort(filenames);
//		System.out.println(Arrays.toString(filenames));
		HashMap<Character, Relation> relations = new HashMap<>();
		int letter = 65;
		for (String filename : filenames)
		{
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line = br.readLine();
			String[] cols = line.split(",");
			
			int columnCount = cols.length;
			int[] max = new int[columnCount];
			int[] min = new int[columnCount];
			for (int i = 0; i < columnCount; i++)
			{
				int value = Integer.parseInt(cols[i]);
				max[i] = value;
				min[i] = value;
			}
//			BufferedDataOutputStream[] bdos = new BufferedDataOutputStream[columnCount];
			DataOutputStream[] dos = new DataOutputStream[columnCount];
			String[] outName = new String[columnCount];
			
			for (int i = 0; i < columnCount; i++)
			{
				outName[i] = (char) letter + ".c" + i;
//				bdos[i] = new BufferedDataOutputStream(new FileOutputStream("out/"+outName[i]));
				dos[i] = new DataOutputStream(new BufferedOutputStream(
						new FileOutputStream("out/"+outName[i])));
				//				System.out.println(outName[i]);
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
						if (max[col]< intToWrite)
						{
							max[col] = intToWrite;
						}
						if (min[col]> intToWrite)
						{
							min[col] = intToWrite;
						}
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
			br.close();
			fr.close();
			for (int i = 0; i < columnCount; i++)
			{
				MAX.put(outName[i], max[i]);
				MIN.put(outName[i], min[i]);
				dos[i].close();
			}
			Relation r = new Relation(outName, rowCount, columnCount);
			//			t.setNumberOfUnique(numberOfUnique);
			relations.put((char) letter, r);
			letter++;
		}
		return relations;
	}
	public static HashMap<Character, Table> composeTable(ArrayList<String> usefulCol,
	                                                     String[] tableNames,
	                                                     HashMap<Character, Relation> r,
	                                                     String[] scans) throws IOException
	{
		HashMap<Character, Table> tableMap = new HashMap<>();
		for (String name : tableNames)
		{
			ArrayList<String> pList = new ArrayList<>();
			char n = name.charAt(0);
			Set<String> completeCol = new HashSet<>();
			for (String col : usefulCol)
			{
				if (col.indexOf(n)>=0)
				{
					completeCol.add(col);
				}
			}
			for (String p : scans)
			{
				if (p.indexOf(n)>=0)
				{
					pList.add(p);
				}
			}
			tableMap.put(n, buildTable(n, completeCol, r.get(n).rowCount, pList));
		}
		return tableMap;
	}
	public static Table buildTable(char n, Set<String> cols, int rowCount,
	                               ArrayList<String> predicates) throws IOException
	{
		String[] allCol = cols.toArray(String[]::new);
		Arrays.sort(allCol);
		int colCount = allCol.length;
//		BufferedDataInputStream[] in = new BufferedDataInputStream[colCount];
		DataInputStream[] in = new DataInputStream[colCount];
		HashMap<String, Integer> indexMap = new HashMap<>();
		for (int i = 0; i < colCount; i++)
		{
//			in[i] = new BufferedDataInputStream(new FileInputStream("out/"+allCol[i]));
			in[i] = new DataInputStream(new BufferedInputStream(
					new FileInputStream("out/"+allCol[i])));
			
			indexMap.put(allCol[i], i);
		}
		ArrayList<int[]> pdc = new ArrayList<>();
		for (String predicate : predicates)
		{
			String[] p = predicate.split(" ");
			int compare = (int) p[1].charAt(0) - 60;
			int target = Integer.parseInt(p[2]);
			int col = indexMap.get(p[0]);
			int[] all = new int[3];
			all[0] = col;
			all[1] = compare;
			all[2] = target;
			pdc.add(all);
		}
		int[] min = new int[colCount];
		int[] max = new int[colCount];
		boolean first = true;
		
		Table t = new Table(String.valueOf(n), colCount, rowCount);
		int tRowCount = 0;
//		System.err.println(rowCount);
		if (rowCount<10000)
		{
			ArrayList<int[]> result = new ArrayList<>();
			for (int i = 0; i < rowCount; i++)
			{
				int[] row = new int[colCount];
				for (int j = 0; j < colCount; j++)
				{
					row[j] = in[j].readInt();
				}
				boolean pass = true;
				for (int[] pList : pdc)
				{
					if (!check(pList, row[pList[0]]))
					{
						pass = false;
					}
				}
				if (pass)
				{
					if (!predicates.isEmpty())
					{
						for (int j = 0; j < colCount; j++)
						{
							if (row[j]>max[j])
							{
								max[j] = row[j];
							}
							if (row[j]<min[j])
							{
								min[j] = row[j];
							}
						}
					}
					result.add(row);
					tRowCount++;
				}
			}
			t.data = result.toArray(new int[rowCount][]);
		}else
		{
//			FileOutputStream out = new FileOutputStream("out/" + n);
//			FileChannel file = out.getChannel();
//			ByteBuffer buf = file.map(FileChannel.MapMode.READ_WRITE, 0, 4 * rowCount * colCount);
//			BufferedDataOutputStream bdos = new BufferedDataOutputStream(new FileOutputStream("out/"+String.valueOf(n)));
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream("out/"+n)));
//			System.out.println(n);
			for (int i = 0; i < rowCount;)
			{
				
				int blockCount = Math.min(rowCount-i, 5000);
				int[][] block = new int[colCount][blockCount];
				for (int j = 0; j < colCount; j++)
				{
					for (int k = 0; k < blockCount; k++)
					{
						block[j][k] = in[j].readInt();
					}
				}
				i = i + blockCount;
				for (int j = 0; j < blockCount; j++)
				{
					int[] row = new int[colCount];
					for (int k = 0; k < colCount; k++)
					{
						
						row[k] = block[k][j];
					}
					boolean pass = true;
					for (int[] pList : pdc)
					{
						if (!check(pList, row[pList[0]]))
						{
							pass = false;
						}
					}
					if (pass)
					{
						if (!predicates.isEmpty())
						{
							if (first)
							{
								first = false;
								min = row.clone();
								max = row.clone();
								for (int k = 0; k < colCount; k++)
								{
									dos.writeInt(row[k]);
								}
							}else {
								for (int k = 0; k < colCount; k++)
								{
									if (row[k]>max[k])
									{
										max[k] = row[k];
									}
									if (row[k]<min[k])
									{
										min[k] = row[k];
									}
									dos.writeInt(row[k]);
								}
							}
						}else
						{
							for (int k = 0; k < colCount; k++)
							{
								dos.writeInt(row[k]);
							}
						}
						tRowCount++;
//						bdos.write(row);
					}
				}
			}
			dos.close();
//			bdos.close();
		}
		if (!predicates.isEmpty())
		{
			t.max = max;
			t.min = min;
			for (int i = 0; i < colCount; i++)
			{
				in[i].close();
			}
		}else
		{
			t.max = new int[colCount];
			t.min = new int[colCount];
			for (int i = 0; i < colCount; i++)
			{
				t.max[i] = MAX.get(allCol[i]);
				t.min[i] = MIN.get(allCol[i]);
			}
		}
		t.rowCount = tRowCount;
		t.indexMap = indexMap;
//		System.err.println(t);
		return t;
	}
	public static boolean check (int[] pList, int value)
	{
		switch(pList[1]) {
			case 0:
				if (value<pList[2])
				{
					return true;
				}
				break;
			case 1:
				if (value==pList[2])
				{
					return true;
				}
				break;
			case 2:
				if (value>pList[2])
				{
					return true;
				}
				break;
		}
		return false;
	}
	public static Table filter(Table t, String preducate) throws IOException
	{
		String fileName = "scan_" + t.getPath();
		String[] p = preducate.split(" = ");
		//A.c45
		long rowCount = 0;
		long tRowCount = t.getRowCount();
		int tColCount = t.getColumnCount();
		int aCol = t.indexMap.get(p[0]);
		int bCol = t.indexMap.get(p[1]);
		int[] min = new int[tColCount];
		int[] max = new int[tColCount];
		boolean first = true;
		ArrayList<int[]> result = new ArrayList<>();
		Table nt;
		if (t.data != null)
		{
			for (int i = 0; i < tRowCount; i++)
			{
				if (t.data[i][aCol] == t.data[i][bCol])
				{
					if (first)
					{
						max = t.data[i].clone();
						min = t.data[i].clone();
						first = false;
					}else {
						for (int j = 0; j < tColCount; j++)
						{
							if (t.data[i][j]>max[j])
							{
								max[j] = t.data[i][j];
							}
							if (t.data[i][j]<min[j])
							{
								min[j] = t.data[i][j];
							}
						}
					}
					result.add(t.data[i]);
					rowCount++;
				}
			}
			nt = new Table("scan_" + t.getPath(), tColCount, rowCount);
			nt.data = result.toArray(new int[(int)rowCount][]);
		}else
		{
//			BufferedDataInputStream in = new BufferedDataInputStream();
			DataInputStream in = new DataInputStream(new BufferedInputStream(
					new FileInputStream("out/"+t.getPath())));
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream("out/"+fileName)));
//			BufferedDataOutputStream bdos = new BufferedDataOutputStream();

			//			System.out.println(fileName);
			for (long i = 0; i < tRowCount; i++)
			{
				int[] row = new int[tColCount];
//				in.read(row);
				for (int j = 0; j < tColCount; j++)
				{
					row[j] = in.readInt();
				}
				if (row[aCol] == row[bCol])
				{
					if (first)
					{
						max = row.clone();
						min = row.clone();
						first = false;
						for (int j = 0; j < tColCount; j++)
						{
							dos.writeInt(row[j]);
						}
					}else {
						for (int j = 0; j < tColCount; j++)
						{
							if (row[j]>max[j])
							{
								max[j] = row[j];
							}
							if (row[j]<min[j])
							{
								min[j] = row[j];
							}
							dos.writeInt(row[j]);
						}
					}
//					bdos.write(row);
					rowCount++;
				}
			}
			in.close();
			dos.close();
			nt = new Table(fileName, tColCount, rowCount);
		}
		nt.max = max;
		nt.min = min;
		nt.indexMap = t.indexMap;
//		System.err.println(nt);
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
//		System.err.println(a.getPath()+" join "+b.getPath());
//		System.err.println(a.toString());
//		System.err.println(b.toString());
		String resultName = a.getPath() + "_and_" + b.getPath();
		long rowCount = 0;
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
		long aRowCount = a.getRowCount();
		int bColCount = b.getColumnCount();
		long bRowCount = b.getRowCount();
		int[] min = new int[aColCount+bColCount];
		int[] max = new int[aColCount+bColCount];
		boolean first = true;
		Table t;
		if (a.data != null && b.data != null)
		{
//			System.err.println("in memory");
			ArrayList<int[]> result = new ArrayList<>();
			for (int i = 0; i < aRowCount; i++)
			{
//				System.err.println(Arrays.toString(a.data[i]));
				for (int j = 0; j < bRowCount; j++)
				{
					if (a.data[i][aCol] == b.data[j][bCol])
					{
//						System.err.println(a.data[i][aCol] + " " + b.data[j][bCol]);
						int[] row = concatenate(a.data[i], b.data[j]);
						if (first)
						{
							max = row.clone();
							min = row.clone();
							first = false;
						}else
						{
							for (int k = 0; k < aColCount+bColCount; k++)
							{
								if (row[k]>max[k])
								{
									max[k] = row[k];
								}
								if (row[k]<min[k])
								{
									min[k] = row[k];
								}
							}
						}
//						System.out.println(Arrays.toString(row));
						result.add(row);
						rowCount++;
					}
				}
			}
			t = new Table(resultName, aColCount+bColCount, rowCount);
			t.data = result.toArray(new int[(int)rowCount][]);
		}else if (b.data != null)
		{
//			BufferedDataInputStream inA = new BufferedDataInputStream(
//					new FileInputStream("out/"+a.getPath()));
			DataInputStream inA = new DataInputStream(new BufferedInputStream(
					new FileInputStream("out/"+a.getPath())));
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream("out/"+resultName)));
//			BufferedDataOutputStream bdos = new BufferedDataOutputStream(
//					new FileOutputStream("out/"+resultName));
//			System.out.println(resultName);
			for (long i = 0; i < aRowCount;)
			{
				//012345678910
				//A.c1 = B.c0
				long aBlockRow = Math.min(aRowCount-i, 10000);
				HashMap<Integer, ArrayList<int[]>> hashBlock = new HashMap<>();
//			    System.out.println(aBlockRow);
				for (int j = 0; j < aBlockRow; j++)
				{
					int[] aRow = new int[aColCount];
//					inA.read(aRow);
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
							int[] row = concatenate(aRow, b.data[j]);
							if (first)
							{
								max = row.clone();
								min = row.clone();
								for (int k = 0; k < aColCount+bColCount; k++)
								{
									dos.writeInt(row[k]);
								}
								first = false;
							}else
							{
								for (int k = 0; k < aColCount+bColCount; k++)
								{
									if (row[k]>max[k])
									{
										max[k] = row[k];
									}
									if (row[k]<min[k])
									{
										min[k] = row[k];
									}
									dos.writeInt(row[k]);
								}
							}
							
//							bdos.write(row);
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
//			BufferedDataInputStream inB = new BufferedDataInputStream(new FileInputStream("out/"+b.getPath()));
			DataInputStream inB = new DataInputStream(new BufferedInputStream(
					new FileInputStream("out/"+b.getPath())));
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream("out/"+resultName)));
//			BufferedDataOutputStream bdos = new BufferedDataOutputStream(new FileOutputStream("out/"+resultName));
//			System.out.println(resultName);
			for (long i = 0; i < bRowCount;)
			{
				//012345678910
				//A.c1 = B.c0
				long BlockRow = Math.min(bRowCount-i, 10000);
				HashMap<Integer, ArrayList<int[]>> hashBlock = new HashMap<>();
//			    System.out.println(aBlockRow);
				for (long j = 0; j < BlockRow; j++)
				{
					int[] bRow = new int[bColCount];
//					inB.read(bRow);
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
							
							int[] row = concatenate(a.data[j], bRow);
							if (first)
							{
								max = row.clone();
								min = row.clone();
								first = false;
								for (int k = 0; k < aColCount+bColCount; k++)
								{
									dos.writeInt(row[k]);
								}
							}else
							{
								for (int k = 0; k < aColCount+bColCount; k++)
								{
									if (row[k]>max[k])
									{
										max[k] = row[k];
									}
									if (row[k]<min[k])
									{
										min[k] = row[k];
									}
									dos.writeInt(row[k]);
								}
							}
//							bdos.write(row);
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
//			BufferedDataInputStream inA = new BufferedDataInputStream(new FileInputStream("out/"+a.getPath()));
			DataInputStream inA = new DataInputStream(new BufferedInputStream(
					new FileInputStream("out/"+a.getPath())));
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream("out/"+resultName)));
			
//			BufferedDataOutputStream bdos = new BufferedDataOutputStream(new FileOutputStream("out/"+resultName));
//			System.out.println(resultName);
			for (long i = 0; i < aRowCount;)
			{
				//012345678910
				//A.c1 = B.c0
				long aBlockRow = Math.min(aRowCount-i, 10000);
				HashMap<Integer, ArrayList<int[]>> hashBlock = new HashMap<>();
//			    System.out.println(aBlockRow);
				for (long j = 0; j < aBlockRow; j++)
				{
					int[] aRow = new int[aColCount];
//					inA.read(aRow);
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
//				BufferedDataInputStream inB = new BufferedDataInputStream(new FileInputStream("out/"+b.getPath()));
				DataInputStream inB = new DataInputStream(new BufferedInputStream(
						new FileInputStream("out/"+b.getPath())));
				for (long j = 0; j < bRowCount; j++)
				{
					int[] bRow = new int[bColCount];
//					inB.read(bRow);
					for (int k = 0; k < bColCount; k++)
					{
						bRow[k] = inB.readInt();
					}
					ArrayList<int[]> intArray = hashBlock.get(bRow[bCol]);
					if (intArray != null)
					{
						for (int[] aRow : intArray)
						{
							int[] row = concatenate(aRow, bRow);
							if (first)
							{
								max = row.clone();
								min = row.clone();
								first = false;
								for (int k = 0; k < aColCount+bColCount; k++)
								{
									dos.writeInt(row[k]);
								}
							}else {
								for (int k = 0; k < aColCount+bColCount; k++)
								{
									if (row[k]>max[k])
									{
										max[k] = row[k];
									}
									if (row[k]<min[k])
									{
										min[k] = row[k];
									}
									dos.writeInt(row[k]);
								}
							}
//							bdos.write(row);
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
		t.max = max;
		t.min = min;
		HashMap<String, Integer> im = new HashMap<>(a.indexMap);
		HashMap<String, Integer> bm = b.indexMap;
		for (String key : bm.keySet())
		{
			im.put(key, bm.get(key) + aColCount);
		}
		t.indexMap = im;
//		fileToDelete.add(t.getPath());
//		System.err.println(t.toString());
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
		long tRowCount = t.getRowCount();
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
			DataInputStream in = new DataInputStream(new BufferedInputStream(
					new FileInputStream("out/"+t.getPath())));
//			BufferedDataInputStream in = new BufferedDataInputStream(
//					new FileInputStream("out/"+t.getPath()));
			for (long i = 0; i < tRowCount; i++)
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
		
		int aCol = a.indexMap.get(as);
		int bCol = b.indexMap.get(bs);
		int aUnique = a.max[aCol] - a.min[aCol];
		int bUnique = b.max[bCol] - b.min[bCol];
//		System.out.println(as + " " + bs);
//		System.out.println(a.toString());
//		System.out.println(aUnique);
//		System.out.println(b.toString());
//		System.out.println(bUnique);
		double result = a.getRowCount() * b.getRowCount() * Math.min(aUnique, bUnique);
		result = result / (aUnique * bUnique);
//		System.out.println(result);
		return result;
		
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









