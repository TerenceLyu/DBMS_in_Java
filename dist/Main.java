import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;

/**
 * TerenceLyu
 * blu96@brandeis.edu
 * cs127_pa3
 * 2019/3/28
 */
public class Main
{
	public static void main(String[] args) throws IOException
	{
		Scanner input = new Scanner(System.in);
		String[] filenames = input.nextLine().split(",");
		HashMap<Character, String> names = new HashMap<>();
		int letter = 65;
		for (String s : filenames)
		{
			names.put((char) letter, s);
			System.out.println(names.get((char) letter));
			letter++;
		}
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
			for (int j = 0; j < tables.length; j++)
			{
				tableReader[j] = new BufferedReader(new FileReader(new File(names.get(tables[j].charAt(0)))));
			}
			
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
//	public static
}
