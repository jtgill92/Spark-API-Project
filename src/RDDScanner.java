import java.io.*;
/*
    Scanner for scala programs using the RDD API

    Possible token types are:
        MetaStatement
        ReservedWord
        Identifier
        Number
        String
        Symbol
        Space
        None

    Token Definitions:
    <letter> --> a|b|...|y|z|A|B|...|Z|_
    <digit> --> 0|1|...|9
    <number> --> <digit>+
    <identifier> --> <letter> (<letter> | <digit>)*
    <string> --> any string between (and including) the closest pair of double quotation marks.
    <char> --> a character between (and including) a pair of single quotation marks.
    <symbol> --> any non-space character that is not a part of other tokens

    @author Jonathan Gill
    Honorable mention to Danny Reinheimer, whose C compiler served as a reference on how to write
    a scanner
 */
public class RDDScanner {
    private FileInputStream fileInput;
    private SimpleToken lastFoundToken;
    private boolean foundToken;
    private char lastReadChar;
    private boolean useLastReadChar;

    /*
        Constructor for the RDDScanner class

        @param fileName is the name of the file to be scanned
     */
    public RDDScanner(String fileName) {
        // Try opening the file
        try {
            fileInput = new FileInputStream(fileName);
        } catch(Exception e){
            System.err.println("Invallid file!");
            System.exit(1);
        }
        lastFoundToken = new SimpleToken("", "None");
        foundToken = false;
        useLastReadChar = false;
    }

    /*
        Retrieves the next character from the file and returns it.
        If the end of the file is reached, the end of file character
        is returned.

        @return Char from file
     */
    private char getNextChar() {
        //TODO
        int chInt;
        try {
            if(useLastReadChar) {
                useLastReadChar = false;
                return lastReadChar;
            }
            // read one character at a time
            if((chInt = fileInput.read()) != -1) {
                return (char) chInt;
            }
        } catch (IOException e) {
            System.err.println("Error reading file in getNextChar.  Not my fault though");
            System.exit(1);
        }
        char ch = '\u001a'; // end of file character
        return ch;
    }

    /*
        Checks the input string against the regular expressions, and
        returns the token found if any

        @return The SimpleToken form of the input String
     */
    private SimpleToken tokenize(String str){

        RDDRegularExpressions re = new RDDRegularExpressions();

        if(re.isMetaStatement(str)) {
            return new SimpleToken(str, "MetaStatement");
        }

        if(re.isReservedWord(str)) {
            return new SimpleToken(str, "ReservedWord");
        }

        if(re.isIdentifier(str)) {
            return new SimpleToken(str, "Identifier");
        }

        if(re.isNumber(str)) {
            return new SimpleToken(str, "Number");
        }

        if(re.isString(str)) {
            return new SimpleToken(str, "String");
        }

        if(re.isSymbol(str)) {
            return new SimpleToken(str, "Symbol");
        }

        if(re.isSpace(str)) {
            return new SimpleToken(str, "Space");
        }

        return new SimpleToken(str, "None");
    }

    /*
        Scans input one character at a time and checks for tokens.
        We save the found toke in the lastTokenFound variable and keep looking
        until we stop finding tokens. If we reach the end of the line and have no tokens
        then report an error.

        @return Token next token from file
     */
    public SimpleToken getNextToken() {
        lastFoundToken = new SimpleToken("", "None"); // reset the token names when we search for new tokens
        SimpleToken currentToken = new SimpleToken("", "None");
        foundToken = false;
        String characters = ""; // all the found characters so far
        char ch;

        // Loop until we reach the end of the file or the end of the line
        while ((ch = getNextChar()) != '\n' && ch != '\u001a'){
            characters += "" + ch;

            // Check for token
            currentToken = tokenize(characters);
            if(!currentToken.type.equals("None")) {
                foundToken = true;
                lastFoundToken = currentToken;
            }

            // If we have already found a token and the current token is none
            // then return the last found token, ignore // comments
            if(foundToken && currentToken.type.equals("None") && !characters.contains("//")) {

                // Save the last character to start from
                lastReadChar = characters.charAt(characters.length() - 1);
                useLastReadChar = true;

                return lastFoundToken;
            }

        }

        // Check to see if we are at the end of the line and if we previously found a token
        // We do this because some tokens require the end of line character.
        // Check for // comments
        if(ch == '\n' && lastFoundToken.type.equals("None") || characters.contains("//")) {
            characters += "" + ch;
            currentToken = tokenize(characters);

            // If we have reached the end of a line and found no tokens, report an error
            if(currentToken.type.equals("None")) {
                System.err.println("Invalid input. Not a valid toke: " + characters);
                System.exit(1);
            }

            // Otherwise, we found a token, so return it
            if(!currentToken.type.equals("None")) {
                useLastReadChar = false;
                return currentToken;
            }
        }

        // If we are at the end of the line, then check for tokens
        if(ch == '\n') {
            currentToken = tokenize(characters);
            if(!currentToken.type.equals("None")) {
                lastReadChar = ch;
                useLastReadChar = true;
                return currentToken;
            }
        }

        // If we have reached the end of the file, check for token
        if(ch == '\u001a' && !lastFoundToken.type.equals("None")) {
            return lastFoundToken;
        }

        return null;
    }
}
