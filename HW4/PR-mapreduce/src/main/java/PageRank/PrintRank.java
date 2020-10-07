package PageRank;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
public class PrintRank {
	public static void main(String[] args) throws FileNotFoundException  {
		System.setOut(new PrintStream(new PrintStream(new BufferedOutputStream(
				new FileOutputStream("/Users/apple/desktop/6240/pr-mapreduce/pagerank.txt")),true)));
		int k = 1000;
		int totalnode = k*k;
		double pr = (double) 1/(totalnode);
		for (int i = 1; i <= totalnode; i ++) {

				System.out.println(String.valueOf(i)+"," + String.valueOf(pr));
			
		}
		
		//System.out.println("Hello World");
	}
	
}
