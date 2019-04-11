/**
 * TerenceLyu
 * blu96@brandeis.edu
 * cs127_pa3
 * 2019/3/28
 */
import java.io.*;
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
	public static Queue<String> fileToDelete = new LinkedList<>();
	public static void main(String[] args) throws IOException
	{
		HashMap<Character, Table> names = handle_Data_Loading();
//		System.out.println("data loaded");
		Scanner input = new Scanner(System.in);
		int numberOfQueries = input.nextInt();
//		for (int i = 0; i < 10; i++)
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
			String[] scans = and.substring(0,and.length()-1).split(" AND ");
//			System.out.println(1 + select);
//			System.out.println(2 + from);
//			System.out.println(3 + where);
//			System.out.println(4 + and);
			String[] tableNames = from.split(", ");
			HashMap<Character, Table> tables = new HashMap<>();
			Table scanned = new Table("X", 0, 0);
			for (int j = 0; j < tableNames.length; j++)
			{
				//enforce the last predicate to reduce table size
				for (int k = 0; k < scans.length; k++)
				{
					if (tableNames[j].charAt(0) == scans[k].charAt(0)){
//						System.out.println("start table scaned");
						scanned = tableScan(names.get(scans[k].charAt(0)), scans[k]);
//						System.out.println("finish table scaned");
						tables.put(tableNames[j].charAt(0), scanned);
					}else
					{
						tables.put(tableNames[j].charAt(0), names.get(tableNames[j].charAt(0)));
					}
				}
			}
//			System.out.println(scanned.start('D'));
//			System.out.println("table scaned");
			//first join
			String[] joins = where.split(" AND ");
			ArrayList<String> joinArray = new ArrayList<>(Arrays.asList(joins));
			Table curr = scanned;
			Table[] firstJoin = new Table[2];
			String[] joinPredicate = new String[2];
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
									joinPredicate[0] = js[0];
									joinPredicate[1] = js[2];
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
									joinPredicate[1] = js[2];
									joinPredicate[0] = js[0];
									joinToRemove = join;
								}
							}
						}
						
					}
				}
			}
//			System.out.println(Arrays.toString(firstJoin));
			Table joinResult = join(joinPredicate[0], joinPredicate[1], firstJoin[0], firstJoin[1]);
			tables.values().remove(firstJoin[0]);
			tables.values().remove(firstJoin[1]);
			joinArray.remove(joinToRemove);
			
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
//		for (Table t: names.values())
//		{
//			File f = new File(t.getPath());
//			f.delete();
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
//			ArrayList<ArrayList<Integer>> diffNum = new ArrayList<>();
			
			int temp = 0;
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
						temp++;
						if (columnCount == 0)
						{
//							diffNum.add(new ArrayList<>());
							if (cb1.charAt(i) == '\n')
							{
								columnCount = temp;
							}
						}
						
						int intToWrite = Integer.parseInt(cb1, startOfNumber, i, 10);
						dos.writeInt(intToWrite);
//						if (!diffNum.get(temp-1).contains(intToWrite))
//						{
//							diffNum.get(temp-1).add(intToWrite);
//						}
						if (cb1.charAt(i) == '\n')
						{
							temp = 0;
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
			fr.close();
			dos.close();
			Table t = new Table(String.valueOf((char) letter), columnCount, rowCount);
//			t.setNumberOfUnique(numberOfUnique);
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
//		ArrayList<ArrayList<Integer>> diffNum = new ArrayList<>();
//		for (int i = 0; i < t.getColumnCount(); i++)
//		{
//			diffNum.add(new ArrayList<>());
//		}
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
//					System.out.println(row[col]);
					for (int j = 0; j < t.getColumnCount(); j++)
					{
//						if (!diffNum.get(j).contains(row[j]))
//						{
//							diffNum.get(j).add(row[j]);
//						}
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
//						if (!diffNum.get(j).contains(row[j]))
//						{
//							diffNum.get(j).add(row[j]);
//						}
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
//						if (!diffNum.get(j).contains(row[j]))
//						{
//							diffNum.get(j).add(row[j]);
//						}
						dos.writeInt(row[j]);
					}
					rowCount++;
				}
			}
		}
		in.close();
		dos.close();
		Table nt = new Table("scan_" + t.getPath(), t.getColumnCount(), rowCount);
		int[] numberOfUnique = new int[t.getColumnCount()];
//		for (int i = 0; i < numberOfUnique.length; i++)
//		{
//			numberOfUnique[i] = diffNum.get(i).size();
//		}
//		nt.setNumberOfUnique(numberOfUnique);
		HashMap<Character, Integer> im = new HashMap<>(t.getIndexMap());
		nt.setIndexMap(im);
//		System.out.println(nt.start('D'));
		fileToDelete.add(nt.getPath());
		return nt;
	}
	public static Table leftDeepJoin(Table t, HashMap<Character, Table> tables, ArrayList<String> joinArray) throws IOException
	{
		while (!joinArray.isEmpty())
		{
			//find a table to join with t
			double minCost = -1;
			String completedJoin = "";
			Table target = new Table();
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
//					System.out.println(temp.toString());
					double cost = cost(t, temp, js[0], js[2]);
					if (minCost == -1 || minCost > cost)
					{
						minCost = cost;
						target = temp;
						completedJoin = join;
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
						completedJoin = join;
					}
				}
			}
//			System.out.println(completedJoin);
			joinArray.remove(completedJoin);
			tables.values().remove(target);
			//check for double join
			ArrayList<String> extraJoin = new ArrayList<>();
			for (String join : joinArray)
			{
				String[] js = join.split(" ");
				char nameA = js[0].charAt(0);
				char nameB = js[2].charAt(0);
				if (t.getPath().indexOf(nameA)>=0 && target.getPath().indexOf(nameB)>=0)
				{
//					System.out.println(join);
					extraJoin.add(join);
				}
				if (t.getPath().indexOf(nameB)>=0 && target.getPath().indexOf(nameA)>=0)
				{
//					System.out.println(join);
					extraJoin.add(join);
				}
			}
			joinArray.removeAll(extraJoin);
			if (extraJoin.size() != 0)
			{
				//if yes, compute double join
				extraJoin.add(completedJoin);
				t = doubleJoin(t, target, extraJoin);
				
			}else
			{
				//else, compute join
				String[] ja = completedJoin.split(" = ");
//				System.out.println(Arrays.toString(ja));
				if (t.getPath().indexOf(ja[0].charAt(0))>=0)
				{
					t = join(ja[0], ja[1], t, target);
				}else
				{
					t = join(ja[1], ja[0], t, target);
				}
			}
		}
		return t;
	}
	public static Table join(String ac, String bc, Table a, Table b) throws IOException
	{
		DataInputStream inA = new DataInputStream(new BufferedInputStream(new FileInputStream(a.getPath())));
		String resultName = a.getPath() + "_and_" + b.getPath();
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(resultName)));
//		System.out.println(ac+" join "+bc);
//		System.out.println(a.toString());
//		System.out.println(b.toString());
//		ArrayList<ArrayList<Integer>> diffNum = new ArrayList<>();
//		for (int i = 0; i < a.getColumnCount()+b.getColumnCount(); i++)
//		{
//			diffNum.add(new ArrayList<>());
//		}
		int rowCount = 0;
		int aCol = a.start(ac.charAt(0)) + Character.getNumericValue(ac.charAt(3));
		int bCol = b.start(bc.charAt(0)) + Character.getNumericValue(bc.charAt(3));
		for (int i = 0; i < a.getRowCount();)
		{
			//012345678910
			//A.c1 = B.c0
			int aBlockRow = Math.min(a.getRowCount()-i, 5000);
			int[][] aBlock = new int[aBlockRow][a.getColumnCount()];
//			System.out.println(aBlockRow);
			for (int j = 0; j < aBlockRow; j++)
			{
				for (int k = 0; k < a.getColumnCount(); k++)
				{
					aBlock[j][k] = inA.readInt();
				}
				i++;
			}
			DataInputStream inB = new DataInputStream(new BufferedInputStream(new FileInputStream(b.getPath())));
			for (int j = 0; j < b.getRowCount(); j++)
			{
				int[] bRow = new int[b.getColumnCount()];
				for (int k = 0; k < b.getColumnCount(); k++)
				{
					bRow[k] = inB.readInt();
				}
				for (int k = 0; k < aBlockRow; k++)
				{
					
					if (aBlock[k][aCol] == bRow[bCol])
					{
//						if (a.getPath().equals("scan_D_and_A"))
//						{
//							System.out.println(a.getPath()+" : "+aCol+" : "+aBlock[k][aCol] + " , " + b.getPath()+" : "+bCol+" : "+bRow[bCol]);
//						}
						for (int l = 0; l < a.getColumnCount(); l++)
						{
//							if (!diffNum.get(l).contains(aBlock[k][l]))
//							{
//								diffNum.get(l).add(aBlock[k][l]);
//							}
							dos.writeInt(aBlock[k][l]);
						}
						for (int l = 0; l < b.getColumnCount(); l++)
						{
//							if (!diffNum.get(a.getColumnCount()+l).contains(bRow[l]))
//							{
//								diffNum.get(a.getColumnCount()+l).add(bRow[l]);
//							}
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
		Table t = new Table(resultName, a.getColumnCount()+b.getColumnCount(), rowCount);
//		int[] numberOfUnique = new int[t.getColumnCount()];
//		for (int i = 0; i < numberOfUnique.length; i++)
//		{
//			numberOfUnique[i] = diffNum.get(i).size();
//		}
//		t.setNumberOfUnique(numberOfUnique);
		HashMap<Character, Integer> im = new HashMap<>(a.getIndexMap());
		HashMap<Character, Integer> bm = b.getIndexMap();
		for (Character key : bm.keySet())
		{
			im.put(key, bm.get(key) + a.getColumnCount());
		}
		t.setIndexMap(im);
		fileToDelete.add(t.getPath());
		return t;
	}
	public static Table doubleJoin(Table a, Table b, ArrayList<String> extra) throws IOException
	{
		DataInputStream inA = new DataInputStream(new BufferedInputStream(new FileInputStream(a.getPath())));
		String resultName = a.getPath() + "_and_" + b.getPath();
//		System.out.println(a.getPath()+" join "+b.getPath());
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(resultName)));
//		ArrayList<ArrayList<Integer>> diffNum = new ArrayList<>();
//		for (int i = 0; i < a.getColumnCount()+b.getColumnCount(); i++)
//		{
//			diffNum.add(new ArrayList<>());
//		}
		int rowCount = 0;
		String[][] predicates = new String[extra.size()][2];
		for (int i = 0; i < extra.size(); i++)
		{
			predicates[i] = extra.get(i).split(" = ");
		}
		for (int i = 0; i < a.getRowCount();)
		{
			//012345678910
			//A.c1 = B.c0
			int aBlockRow = Math.min(a.getRowCount()-i, 5000);
			int[][] aBlock = new int[aBlockRow][a.getColumnCount()];
//			System.out.println(aBlockRow);
			for (int j = 0; j < aBlockRow; j++)
			{
				for (int k = 0; k < a.getColumnCount(); k++)
				{
					aBlock[j][k] = inA.readInt();
				}
				i++;
			}
			DataInputStream inB = new DataInputStream(new BufferedInputStream(new FileInputStream(b.getPath())));
			for (int j = 0; j < b.getRowCount(); j++)
			{
				int[] bRow = new int[b.getColumnCount()];
				for (int k = 0; k < b.getColumnCount(); k++)
				{
					bRow[k] = inB.readInt();
				}
				for (int k = 0; k < aBlockRow; k++)
				{
					boolean pass = true;
					for (String[] p : predicates)
					{
						int aCol = 0, bCol = 0;
						if (a.getPath().indexOf(p[0].charAt(0))>=0)
						{
							aCol = a.start(p[0].charAt(0)) + Integer.parseInt(p[0].substring(3));
							bCol = b.start(p[1].charAt(0)) + Integer.parseInt(p[1].substring(3));
						}else
						{
							aCol = a.start(p[1].charAt(0)) + Integer.parseInt(p[1].substring(3));
							bCol = b.start(p[0].charAt(0)) + Integer.parseInt(p[0].substring(3));
						}
						if (aBlock[k][aCol] != bRow[bCol])
						{
							pass = false;
						}
					}
					if (pass)
					{
//						if (a.getPath().equals("scan_D_and_A"))
//						{
//							System.out.println(a.getPath()+" : "+aCol+" : "+aBlock[k][aCol] + " , " + b.getPath()+" : "+bCol+" : "+bRow[bCol]);
//						}
						for (int l = 0; l < a.getColumnCount(); l++)
						{
//							if (!diffNum.get(l).contains(aBlock[k][l]))
//							{
//								diffNum.get(l).add(aBlock[k][l]);
//							}
							dos.writeInt(aBlock[k][l]);
						}
						for (int l = 0; l < b.getColumnCount(); l++)
						{
//							if (!diffNum.get(a.getColumnCount()+l).contains(bRow[l]))
//							{
//								diffNum.get(a.getColumnCount()+l).add(bRow[l]);
//							}
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
		Table t = new Table(resultName, a.getColumnCount()+b.getColumnCount(), rowCount);
//		int[] numberOfUnique = new int[t.getColumnCount()];
//		for (int i = 0; i < numberOfUnique.length; i++)
//		{
//			numberOfUnique[i] = diffNum.get(i).size();
//		}
//		t.setNumberOfUnique(numberOfUnique);
		HashMap<Character, Integer> im = new HashMap<>(a.getIndexMap());
		HashMap<Character, Integer> bm = b.getIndexMap();
		for (Character key : bm.keySet())
		{
			im.put(key, bm.get(key) + a.getColumnCount());
		}
		t.setIndexMap(im);
		fileToDelete.add(t.getPath());
		return t;
	}
	public static void sum(Table t, String[] sums) throws IOException
	{
		long[] result = new long[sums.length];
		DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(t.getPath())));
//		System.out.println(t.getRowCount());
		for (int i = 0; i < t.getRowCount(); i++)
		{
			//0123456789
			//SUM(D.c0)
			//SUM(D.c4)
			//SUM(C.c12)
//			System.out.println(t.getColumnCount());
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
			if (t.getRowCount() != 0)
			{
				System.out.print(result[i]);
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
//		int aCol = a.start(as.charAt(0)) + Integer.parseInt(as.substring(3));
//		int bCol = b.start(bs.charAt(0)) + Integer.parseInt(bs.substring(3));
//		int aUnique = a.getNumberOfUnique()[aCol];
//		int bUnique = b.getNumberOfUnique()[bCol];
////		System.out.println(aUnique);
////		System.out.println(bUnique);
//		double result = a.getRowCount() * b.getRowCount() * Math.min(aUnique, bUnique);
//		result = result / (aUnique * bUnique);
//		System.out.println(result);
		return Math.min(a.getColumnCount(), b.getColumnCount());
	}
}









