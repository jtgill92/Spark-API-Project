@author Jonathan Gill

Spark API Project Part 2

RDDToDataframe is a source-to-source compiler written in Java that takes as input a scala program
using the Spark RDD API to an equivalent scala program using the Dataframe API. The program is
made of 5 software components:

  SimpleToken.java:
    A simple token class. It has two public fields, both Strings, named word and type.
    word stores the text of a token, and type stores the token's type.

  RDDRegularExpressions.java
    A class that handles matching a string of characters against regular expressions
    to test for token membership.

  RDDScanner.java
    The Scanner portion of the compiler. It is hand-coded. It reads the input file one character
    at a time and matches the current set of chars against the regular expressions in
    RDDRegularExpressions.java. If a token is found it is fed into the parser, and
    the pointer is moved forward in the file allowing a different token to be
    read next time.

  RDDParser.java
    The Parser portion of the compiler. It is a hand-coded recursive-decent parser.
    Tokens are matched against production rules in the grammer to determine meaning.
    It uses ad hoc systax-driven translation to produce the output scala file.

  RDDToDataframe.java
    The main function of the program. It takes as input a filename and creates an instance of
    RDDScanner feeding in that file to the scanner. Next an instance of the RDDParser is
    created and the scanner is passed to it. When the parser has finished, it retrieves the
    output from the parser and writes it to the output file.

How to install:
  This program is ditributed in a .tar file along with the other parts of the 
  Spark API project. Once the main .tar file has been extracted, all that is
  left is to compile the program.

  (Note: This program was built and tested with Java version 1.8.0_181 and
  Ubuntu 18.04. However, the program does not use any features exclusive to
  to these tools and should also run on older versions of both Ubuntu and Java.)

  To compile:
  1) Open a terminal and navigate to CSC512_p2/src
  2) Enter the command: javac RDDToDataframe.java

How to use:
  The program can compile one or more files at a time, passing them as arguments
  in the command line. It takes in scala programs that use the RDD API and
  outputs equivalent scala programs using the Dataframe API.

  Program Input:
  The input should be one or more .scala files containing a scala program written using
  the Spark RDD API.

  Program Output:
  The compiler will output a message in the terminal as well as an output file.
  The compiler will output a message in the terminal indicating whether or
  not the compilating was successful. If the parser encounters an error it
  it will output the input file up to where the error was encountered. Otherwise,
  it will output the entire input file read. If the parse succeeds, it will
  attempt to create the output file. If the creation of the output file succeeds
  the contents of the output file will also be displayed in the terminal.
  The output files will be located in the same directory as the input files used.

    Terminal:
      File Input:
      <input file text>
      Parsing succesful
      File Output:
      <output file text>

    Files:
      <input file name>_output_in_dataframe.scala

  Included test files:
    prog1.scala
    prog2.scala
    prog3.scala -> Prog4_RDD from Part 0
    prog4.scala -> Prog5_RDD from Part 0
    prog5.scala -> Prog6_RDD from Part 0

  To run the program:
  1) Open a terminal and navigate to CSC512_p2/src
  2) Enter the command: java RDDToDataframe <scala_program_path> <options>

  Example(One File):
  $ java RDDToDataframe ../test/prog6.scala

  Program Output:
    File Input:
    sc.range(3,7)
      .map(num=>{val j = num%2; if(j == 0) num + num - 1 else if(num == 7) num + num - 1 else num})
      .collect()
    Parsing was successful
    File Output:
    spark.range(3,7).selectExpr("id as _1")
         .selectExpr("if(_1%2==0,_1+_1-1,if(_1==7,_1+_1-1,_1)) as _1")
         .collect()

    prog6_output_in_dataframe.scala will be created and contain the file output text:
    spark.range(3,7).selectExpr("id as _1")
         .selectExpr("if(_1%2==0,_1+_1-1,if(_1==7,_1+_1-1,_1)) as _1")
         .collect()

How to Test:
  You can easily test the program against all the included test files at once:
  1) Open a terminal and navigate to CSC512_p2/src
  2) Copy and paste the command(excluding the $):
  $ java RDDToDataframe ../test/*.scala
  
