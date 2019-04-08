/**
 * TerenceLyu
 * blu96@brandeis.edu
 * cs127_pa3
 * 2019/3/28
 */
import java.io.*;
import java.nio.CharBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;
public class Main
{
	public static void main(String[] args) throws IOException
	{
		HashMap<Character, Table> names = handle_Data_Loading();
		
		Scanner input = new Scanner(System.in);
		int numberOfQueries = input.nextInt();
		for (int i = 0; i < numberOfQueries; i++)
		{
			input.nextLine();//skip empty line
			String select = input.nextLine().split(" ", 2)[1];
			String from = input.nextLine().split(" ", 2)[1];
			String where = input.nextLine().split(" ", 2)[1];
			String and = input.nextLine().split(" ", 2)[1];
			String[] tables = from.split(", ");
			BufferedReader[] tableReader = new BufferedReader[tables.length];
//			for (int j = 0; j < tables.length; j++)
//			{
//				tableReader[j] = new BufferedReader(new FileReader(
//						new File(names.get(tables[j].charAt(0)).getPath())));
//			}
			Table newT = tableScan(names.get(and.charAt(0)), and);
			System.out.println(1 + select);
			System.out.println(2 + from);
			System.out.println(3 + where);
			System.out.println(4 + and);
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
		String extension = ".txt";
		for (String filename : filenames)
		{
			FileReader fr = new FileReader(new File(filename));
			FileOutputStream fos = new FileOutputStream((char) letter + extension);
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fos));
			CharBuffer cb1 = CharBuffer.allocate(4 * 1024);
			CharBuffer cb2 = CharBuffer.allocate(4 * 1024);
			int temp = 1;
			int columnCount = 0;
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
						int intToWrite = Integer.parseInt(cb1, startOfNumber, i, 10);
						dos.writeInt(intToWrite);
						startOfNumber++;
					}
				}
				cb2.clear();
				cb2.append(cb1, startOfNumber, cb1.length());
				CharBuffer tmp = cb2;
				cb2 = cb1;
				cb1 = tmp;
			}
			fr.close();
			fos.close();
			dos.close();
			Table t = new Table((char) letter + extension, columnCount);
			tables.put((char) letter, t);
			letter++;
		}
		return tables;
	}
	public static Table tableScan(Table t, String predicate) throws IOException
	{
		Scanner scanner = new Scanner(new File(t.getPath()));
		FileOutputStream fos = new FileOutputStream(t.getPath()+ "_scan" + ".txt");
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fos));
		while (scanner.hasNext())
		{
			int[] row = new int[t.getColumnCount()];
			for (int i = 0; i < t.getColumnCount(); i++)
			{
				row[i] = scanner.nextInt();
			}
			String[] p = predicate.substring(0,predicate.length()-2).split(" ");
			int col = Integer.parseInt(p[0].substring(3));
			char compare = p[2].charAt(0);
			
			if (compare == '=')
			{
				if (row[col] == Integer.parseInt(p[2]))
				{
					for (int i = 0; i < t.getColumnCount(); i++)
					{
						dos.writeInt(row[i]);
					}
				}
			}
			if (compare == '>')
			{
				if (row[col] > Integer.parseInt(p[2]))
				{
					for (int i = 0; i < t.getColumnCount(); i++)
					{
						dos.writeInt(row[i]);
					}
				}
			}
			if (compare == '<')
			{
				if (row[col] < Integer.parseInt(p[2]))
				{
					for (int i = 0; i < t.getColumnCount(); i++)
					{
						dos.writeInt(row[i]);
					}
				}
			}
		}
		fos.close();
		dos.close();
		return new Table(t.getPath()+ "_scan" + ".txt", t.getColumnCount());
	}
}









