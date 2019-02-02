//import java.util.Scanner;
import java.io.*;

/*
    Converts a scala program using the RDD API to one using the Dataset API

    @author Jonathan Gill
 */
public class RDDToDataframe {
    public static void main(String[] args) {
        // checks to see if we are given any arguments
        if(args.length < 1) {
            System.err.println("Please provide an input file to process");
            System.exit(1);
        }
        for (String fileName: args) {

            try {
                // get the file name minus the dot
                int pos = fileName.lastIndexOf(".");
                String newFileName = fileName.substring(0, pos) + "_output_in_dataframe.scala";
                PrintWriter writer = new PrintWriter(newFileName, "UTF-8");

                RDDParser parser = new RDDParser();
                RDDScanner scanner = new RDDScanner(fileName);
                if (parser.parse(scanner)) {
                    System.out.println("\nParsing was successful");
                    System.out.println("File Output:");
                    String out = parser.getOutput();
                    writer.print(out);
                    System.out.println(out);
                    // parser.printUDFSymbolTables();
                } else {
                    System.out.println("\nParsing error");
                }

                writer.close();
            } catch (FileNotFoundException e) {
                System.err.println("Could not create output file");
                System.exit(1);
            } catch (UnsupportedEncodingException e) {
                System.err.println("Error encoding output file.  Not my fault though");
                System.exit(1);
            }
        }
    }
}
