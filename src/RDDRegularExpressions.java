import java.io.*;
/*
    This class implements the regular expressions used for the RDDScanner class

    @author Jonathan Gill
    Honorable mention to Danny Reinheimer, whose C compiler served as a reference on how to write
    a scanner
 */
public class RDDRegularExpressions {

    // Array of reserved words
    private String[] reservedWords = {
            "int",
            "void",
            "if",
            "while",
            "return",
            "write",
            "print",
            "continue",
            "break",
            "binary",
            "decimal",
            "val"
    };

    // Array of symbols
    private String[] symbols = {
            "(", ")",
            "{", "}",
            "[", "]",
            ",", ";",
            "+", "-",
            "*", "/",
            "==", "!=",
            ">", ">=",
            "<", "<=",
            "&&", "||",
            "=", ".",
            "%", "=>"
    };

    /*
        Checks to see if the imput matches white space.
        White space can be a tab, space, or newline.

        @param str is the String to be checked
        @return boolean indicating if it is a space
     */
    public boolean isSpace(String str) {
        char ch = str.charAt(0);
        for (int i = 0; i < str.length(); i++) {
            ch = str.charAt(i);
            if(ch != ' ' && ch != '\n' && ch != '\r' && ch != '\u001a' && ch != '\t') {
                // this is not space
                return false;
            }
        }
        return true;
    }

    /*
        Checks to see if the character is a digit

        @param c is the character to check
        @return boolean indicating if it is a digit
     */
    private boolean isDigit(char c) {
        return (c >= '0' && c <= '9');
    }

    /*
        Checks to see if the string matches the patter for an identifier
        The pattern is: Letter(Letter|Digit)*

        @param str The String to be checked
        @return boolean indicating if it is an identifier
     */
    public boolean isIdentifier(String str) {
        char ch = str.charAt(0);
        if(isLetter(ch)) {
            for(int i = 1; i < str.length(); i++) {
                char c = str.charAt(i);
                if(!(isLetter(c) || isDigit(c))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /*
        Checks to see if a character is a letter

        @param c the character to check
        @return boolean indicating if it is a letter
     */
    private boolean isLetter(char c) {
        return ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_');
    }

    /*
        Checks to see if the string matches the pattern for a number
        The pattern is: Digit+

        @param str The String to be checked
        @return boolean indicating if it is a number
     */
    public boolean isNumber(String str) {
        char ch = str.charAt(0);

        // if the first character is a digit
        if(isDigit(ch)) {
            for(int i = 1; i < str.length(); i++) {
                ch = str.charAt(i);
                if(!isDigit(ch)) {
                    // this is not a number
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /*
        Checks to see if the string matches the pattern for a reserved word by comparing
        it to elements of the reserved word array defined above

        @param str The String to be checked
        @return boolean indicating if it is a reserved word
     */
    public boolean isReservedWord(String str) {
        // Iterate through the list of reserve words to see if the string equals any of them
        for(String word: reservedWords) {
            if(str.equals(word)) {
                // This is a reserved word
                return true;
            }
        }
        // If no matches then not a reserved word
        return false;
    }

    /*
        Checks to see if the string matches the pattern for a symbol by comparing it to
        elements of the symbol array defined above

        @param str The String to be checked
        @return boolean indicating if it is a symbol
     */
    public boolean isSymbol(String str) {
        // Iterate through the list of symbols to see if the string equals any of them
        for(String symbol: symbols) {
            if(str.equals(symbol)) {
                // This is a symbol
                return true;
            }
        }
        // if no matches then not a symbol
        return false;
    }

    /*
        Checks to see if the input matches the pattern for a String
	    The pattern for a string is that it starts and ends with quotations

	    @param str The input String to be checked
	    @return boolean indicating if it is a string
     */
    public boolean isString(String str) {
        char ch = str.charAt(0);
        char chEnd = str.charAt(str.length() -1);  // gets the last character in the string
        // Check to see if it starts with a quotation mark
        if(ch == '"' && str.length() != 1 && chEnd == '"') {
            // this is a string
            return true;
        }
        return false;
    }

    /*
        Checks to see if the input matches the pattern for a meta statement

	    @param str The input String to be checked
	    @return boolean indicating if it is a meta statement
     */
    public boolean isMetaStatement(String str) {
        char ch = str.charAt(0);
        // Check to see if we are starting with a # and there is something after it
        if(ch == '#' && str.length() > 1) {
            // Check to see if we end with a newline
            if(str.charAt(str.length() -1) == '\n') {
                // this is a meta statement
                return true;
            }
            return false;
        }

        // Check to see if we start with //
        if(ch == '/' && str.length() > 1) {
            // if the second character is / and we end with newline
            if(str.charAt(1) == '/' && str.charAt(str.length() -1) == '\n') {
                return true;
            }
        }
        return false;
    }
}
