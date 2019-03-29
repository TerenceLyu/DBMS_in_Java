import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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
		String[] filenames = input.next().split(",");
		BufferedReader[] tableReader = new BufferedReader[filenames.length];
		File[] f = new  File[filenames.length];
		int index = 0;
		for (String filename : filenames)
		{
			f[]
			tableReader[index] = new BufferedReader(new FileReader(new File(filename)));
		}
		
	}
	public static
}
