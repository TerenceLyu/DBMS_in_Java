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
			Table curr = scanned;
			int minCol = -1;
			for (Table t : tables.values())
			{
				if (minCol<0 || minCol>t.getColumnCount())
				{
					minCol = t.getColumnCount();
					curr = t;
				}
			}
			System.out.println(curr);
			String[] joins = where.split(" AND ");
			ArrayList<String> joinArray = new ArrayList<>(Arrays.asList(joins));
			while (!joinArray.isEmpty())
			{
				for (int j = 0; j < joinArray.size(); j++)
				{
					String join = joinArray.get(j);
					String[] jl = join.split(" ");
					char nameA = jl[0].charAt(0);
					char nameB = jl[2].charAt(0);
					if (curr.getPath().indexOf(nameA) >= 0 && curr.getPath().indexOf(nameB) >= 0)
					{
						curr = filter(curr, join);
						joinArray.remove(join);
						j--;
					}
				}
				double minCost = -1;
				String[] pr = new String[2];
				Table t = curr;
				String joinString = "";
				for (int j = 0; j < joinArray.size(); j++)
				{
					String join = joinArray.get(j);
					String[] jl = join.split(" ");
					char nameA = jl[0].charAt(0);
					char nameB = jl[2].charAt(0);
					String currPath = curr.getPath();
					if (currPath.indexOf(nameA)>=0)
					{
						t = tables.get(nameB);
						System.out.println(t.toString());
						double joinCost = cost(curr, t, jl[0], jl[2]);
						System.out.println(joinCost);
						if (minCol < 0 || minCost > joinCost)
						{
							System.out.println("cool");
							minCost = joinCost;
							pr[0] = jl[0];
							pr[1] = jl[2];
							joinString = join;
						}
					}
					if (currPath.indexOf(nameB)>=0)
					{
						t = tables.get(nameA);
						System.out.println(t.toString());
						double joinCost = cost(curr, t, jl[2], jl[0]);
						System.out.println(joinCost);
						if (minCol < 0 || minCost > joinCost)
						{
							System.out.println("cool");
							minCost = joinCost;
							pr[1] = jl[0];
							pr[0] = jl[2];
							joinString = join;
						}
					}
				}
				curr = join(pr[0], pr[1], curr, t);
				joinArray.remove(joinString);
			}
//			LinkedList<String> joinList = new LinkedList<>(Arrays.asList(joins));
//
//			//0123456789
//			//A.c30 = D.c10
//
//			Queue<String> joinQueue = new LinkedList<>();
//
//			while (!joinList.isEmpty())
//			{
//				LinkedList<String> thingsToRemove = new LinkedList<>();
//				for (String j : joinList)
//				{
//					if (j.indexOf(curr.getPath().charAt(5)) >= 0)
//					{
//						joinQueue.add(j);
//						thingsToRemove.add(j);
//					}else
//					{
//						LinkedList<String> thingsToAdd = new LinkedList<>();
//						for (String s : joinQueue)
//						{
//							String[] aj = j.split(" ");
//							if (s.indexOf(aj[0].charAt(0)) >= 0)
//							{
//								thingsToAdd.add(j);
//								thingsToRemove.add(j);
//							}
//							if (s.indexOf(aj[2].charAt(0)) >= 0)
//							{
//								thingsToAdd.add(j);
//								thingsToRemove.add(j);
//							}
//						}
//						joinQueue.addAll(thingsToAdd);
//					}
//				}
//				joinList.removeAll(thingsToRemove);
//			}
//
//			for (String join : joinQueue)
//			{
//				//A.c3 = D.c0
//				String[] jl = join.split(" ");
//				char nameA = join.charAt(0);
//				char nameB = join.charAt(7);
//				if (curr.getPath().indexOf(nameA) >= 0)
//				{
//					Table t = tables.get(nameB);
//					if (curr.getPath().indexOf(nameB) >= 0)
//					{
//						curr = filter(curr, join);
//					}else
//					{
//						curr = join(jl[0], jl[2], curr, t);
//					}
//				}else
//				{
//					Table t = tables.get(nameA);
//					if (curr.getPath().indexOf(nameA) >= 0)
//					{
//						curr = filter(curr, join);
//					}else
//					{
//						curr = join(jl[2], jl[0], curr, t);
//					}
//				}
////				System.out.println("table joined");
//			}
			//sum
			//SUM(D.c0), SUM(D.c4), SUM(C.c1)
			String[] sums = select.split(", ");
			
//			System.out.println("***********");
			sum(curr, sums);
//			System.out.println("***********");
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
			ArrayList<ArrayList<Integer>> diffNum = new ArrayList<>();
			
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
							diffNum.add(new ArrayList<>());
							if (cb1.charAt(i) == '\n')
							{
								columnCount = temp;
							}
						}
						
						int intToWrite = Integer.parseInt(cb1, startOfNumber, i, 10);
						dos.writeInt(intToWrite);
						if (!diffNum.get(temp-1).contains(intToWrite))
						{
							diffNum.get(temp-1).add(intToWrite);
						}
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
			int[] numberOfUnique = new int[columnCount];
			for (int i = 0; i < columnCount; i++)
			{
				numberOfUnique[i] = diffNum.get(i).size();
			}
//			System.out.println(Arrays.toString(numberOfUnique));
			fr.close();
			dos.close();
			Table t = new Table(String.valueOf((char) letter), columnCount, rowCount);
			t.setNumberOfUnique(numberOfUnique);
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
		ArrayList<ArrayList<Integer>> diffNum = new ArrayList<>();
		for (int i = 0; i < t.getColumnCount(); i++)
		{
			diffNum.add(new ArrayList<>());
		}
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
						if (!diffNum.get(j).contains(row[j]))
						{
							diffNum.get(j).add(row[j]);
						}
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
						if (!diffNum.get(j).contains(row[j]))
						{
							diffNum.get(j).add(row[j]);
						}
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
						if (!diffNum.get(j).contains(row[j]))
						{
							diffNum.get(j).add(row[j]);
						}
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
		for (int i = 0; i < numberOfUnique.length; i++)
		{
			numberOfUnique[i] = diffNum.get(i).size();
		}
		nt.setNumberOfUnique(numberOfUnique);
		HashMap<Character, Integer> im = new HashMap<>(t.getIndexMap());
		nt.setIndexMap(im);
//		System.out.println(nt.start('D'));
		fileToDelete.add(nt.getPath());
		return nt;
	}
	public static Table join(String ac, String bc, Table a, Table b) throws IOException
	{
		DataInputStream inA = new DataInputStream(new BufferedInputStream(new FileInputStream(a.getPath())));
		String resultName = a.getPath() + "_and_" + b.getPath();
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(resultName)));
		System.out.println(ac+" join "+bc);
		System.out.println(a.toString());
		System.out.println(b.toString());
		ArrayList<ArrayList<Integer>> diffNum = new ArrayList<>();
		for (int i = 0; i < a.getColumnCount()+b.getColumnCount(); i++)
		{
			diffNum.add(new ArrayList<>());
		}
		int rowCount = 0;
		int aCol = a.start(ac.charAt(0)) + Character.getNumericValue(ac.charAt(3));
		int bCol = b.start(bc.charAt(0)) + Character.getNumericValue(bc.charAt(3));
		for (int i = 0; i < a.getRowCount();)
		{
			//012345678910
			//A.c1 = B.c0
			int aBlockRow = Math.min(a.getRowCount()-i, 2000);
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
							if (!diffNum.get(l).contains(aBlock[k][l]))
							{
								diffNum.get(l).add(aBlock[k][l]);
							}
							dos.writeInt(aBlock[k][l]);
						}
						for (int l = 0; l < b.getColumnCount(); l++)
						{
							if (!diffNum.get(a.getColumnCount()+l).contains(bRow[l]))
							{
								diffNum.get(a.getColumnCount()+l).add(bRow[l]);
							}
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
		int[] numberOfUnique = new int[t.getColumnCount()];
		for (int i = 0; i < numberOfUnique.length; i++)
		{
			numberOfUnique[i] = diffNum.get(i).size();
		}
		t.setNumberOfUnique(numberOfUnique);
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
	public static Table filter(Table t, String predicate) throws IOException
	{
		DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(t.getPath())));
		FileOutputStream fos = new FileOutputStream("scan_" + t.getPath());
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fos));
		//01234
		//C.c2
		// =
		//D.c2
		String[] p = predicate.split(" ");
		String a = p[0];
		String b = p[2];
		int aCol = t.start(a.charAt(0)) + Integer.parseInt(a.substring(3));
		int bCol = t.start(b.charAt(0)) + Integer.parseInt(b.substring(3));
//		System.out.println(a+" "+t.start(a.charAt(0))+" "+aCol);
//		System.out.println(b+" "+t.start(b.charAt(0))+" "+bCol);
		ArrayList<ArrayList<Integer>> diffNum = new ArrayList<>();
		for (int i = 0; i < t.getColumnCount(); i++)
		{
			diffNum.add(new ArrayList<>());
		}
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
					if (!diffNum.get(j).contains(row[j]))
					{
						diffNum.get(j).add(row[j]);
					}
					dos.writeInt(row[j]);
				}
				rowCount++;
			}
			
		}
		in.close();
		dos.close();
		Table nt = new Table("scan_" + t.getPath(), t.getColumnCount(), rowCount);
		HashMap<Character, Integer> im = new HashMap<>(t.getIndexMap());
		nt.setIndexMap(im);
		int[] numberOfUnique = new int[t.getColumnCount()];
		for (int i = 0; i < numberOfUnique.length; i++)
		{
			numberOfUnique[i] = diffNum.get(i).size();
		}
		nt.setNumberOfUnique(numberOfUnique);
		fileToDelete.add(nt.getPath());
		return nt;
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
		int aCol = a.start(as.charAt(0)) + Integer.parseInt(as.substring(3));
		int bCol = b.start(bs.charAt(0)) + Integer.parseInt(bs.substring(3));
		int aUnique = a.getNumberOfUnique()[aCol];
		int bUnique = b.getNumberOfUnique()[bCol];
//		System.out.println(aUnique);
//		System.out.println(bUnique);
		double result = a.getRowCount() * b.getRowCount() * Math.min(aUnique, bUnique);
		result = result / (aUnique * bUnique);
//		System.out.println(result);
		return result;
	}
}









