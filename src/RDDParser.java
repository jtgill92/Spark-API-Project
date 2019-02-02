import java.util.*;
/*
    Parser for scala programs using the RDD API

    The parser is implemented by way of recursive decent and is meant to be used with the
    RDDScanner class. It parses an input scala program written in the RDD API to one
    using the Dataframe API instead. Output can be retrieved via the getOutput function.

    Grammar:
    <Program>       ::= sc.range(<number>,<number>)<MapOps>.collect()
    <MapOps>        ::= ∅
                      | <MapOps>.map(<UDF>)
    <UDF>           ::= <identifier> => <Expression>
    <Expression>    ::= {<ComplexExpr>}
                      | <SimpleExpr>
    <SimpleExpr>    ::= <PureExpr>
                      | (<TupleExpr>)
    <TupleExpr>     ::= <PureExpr>, <PureExpr>
                      | <TupleExpr>, <PureExpr>
    <ComplexExpr>   ::= <SimpleExpr>
                      | <AssignExprs>;<SimpleExpr>
    <AssignExprs>   ::= <AssignExpr>
                      | <AssignExprs>;<AssignExpr>
    <AssignExpr>    ::= val <identifier> = <PureExpr>
    <PureExpr>      ::= <identifier>
                      | <identifier>.<identifier>
                      | (<PureExpr>)
                      | <PureExpr> <Op> <PureExpr>
                      | if ( <CompExpr>) <PureExpr> else <PureExpr>
    <CompExpr>      ::= <PureExpr> <Comp> <PureExpr>
    <Op>            ::= + | - | * | %
    <Comp>          ::= == | < | > | != | >= | <=

    @author Jonathan Gill
 */
public class RDDParser {
    private boolean endState;
    private RDDScanner scanner;
    private SimpleToken currentToken;
    private String out;
    private Stack<SimpleToken> UDFStack;
    private HashMap<String, String> symbolTable;
    private String SQL;
    private List<HashMap> UDFSymbolTables;
    private Boolean inAssignExpr;
    private int tupleCount;

    // boolean EOF;

    /*
        Returns the output text containing scala code now using
        the Dataframe API

        @output String that contains output file text.
     */
    public String getOutput() {
        if(!endState) {
            System.err.println("ERROR: Tried to generate output for an incomplete or failed parse");
            System.exit(1);
        }
        return out;
    }

    /*
        Prints to the console the contents of the symbol tables used in the
        UDF to SQL translation.

        @output void
     */
    public void printUDFSymbolTables() {
        int i = 1;
        for (HashMap table: UDFSymbolTables) {

            System.out.println("Symbol Table " + i + ":");
            Iterator itr = table.keySet().iterator();
            while (itr.hasNext()) {
                String key = (String) itr.next();
                String val = (String) table.get(key);

                System.out.println("key: " + key + ", val: " + val);
            }
            i++;
        }
    }

    /*
        The main purpose of the class.
        This function initializes the global variables and
        triggers the recursive-decent parsing algorithm.

        @output Boolean true if the parsing completed succesfully, false if
        the parser encountered and error or otherwise failed to finish.
     */
    public boolean parse(RDDScanner scanner) {
        System.out.println("File Input:");
        endState = false;
        this.scanner = scanner;
        out = "";
        UDFStack = new Stack<>();
        symbolTable = new HashMap<>();
        SQL = "";
        UDFSymbolTables = new ArrayList<>();
        inAssignExpr = false;
        tupleCount = 0;

        // EOF = false;

        return program();
    }

    /*
        Retrieves the next token from the scanner and stores it in the global variable
        currentToken. Returns true if there are more tokens a valid token was retrieved.
        Returns false otherwise.

        @output boolean indicating if another valid token was found
     */
    private boolean getNextToken() {
        SimpleToken token;

/*      // allows us to detect an end-of-file token
        if(!EOF) {
            if ((token = scanner.getNextToken()) == null) {
                currentToken = new SimpleToken("", "EOF");
                EOF = true;
                return true;
            } else {
                currentToken = token;
                return true;
            }
        }
*/
        if((token = scanner.getNextToken()) != null) {
            currentToken = token;
            System.out.print(currentToken.word);
            if(currentToken.type.equals("Space")) { // ignore whitespace
                getNextToken();
            }
            return true;
        }
        return false;
    }

    /*
        The following functions implement the recursive-decent parsing algorithm.
        They use ad-hoc syntax-directed translation to produce the output code.

        @output boolean True if the production rule was matched. False otherwise.
     */
    private boolean program() {
        if(getNextToken()) {
            if(currentToken.word.equals("sc")) {
                out += "spark";
                if(getNextToken()){
                    if(currentToken.word.equals(".")) {
                        out += ".";
                        if(getNextToken()){
                            if(currentToken.word.equals("range")) {
                                out += "range";
                                if(getNextToken()){
                                    if(currentToken.word.equals("(")) {
                                        out += "(";
                                        if(getNextToken()){
                                            if(currentToken.type.equals("Number")) {
                                                out += currentToken.word;
                                                if(getNextToken()){
                                                    if(currentToken.word.equals(",")) {
                                                        out += ",";
                                                        if(getNextToken()){
                                                            if(currentToken.type.equals("Number")) {
                                                                out += currentToken.word;
                                                                if(getNextToken()){
                                                                    if(currentToken.word.equals(")")) {
                                                                        out += ").selectExpr(\"id as _1\")";
                                                                        if(getNextToken()){
                                                                            if(mapOps()) {
                                                                                if(currentToken.word.equals("collect")) {
                                                                                    out += "collect";
                                                                                    if(getNextToken()){
                                                                                        if(currentToken.word.equals("(")) {
                                                                                            out += "(";
                                                                                            if(getNextToken()){
                                                                                                if(currentToken.word.equals(")")) {
                                                                                                    out += ")";
                                                                                                    endState = true;
                                                                                                    return true;
                                                                                                }
                                                                                            }
                                                                                        }
                                                                                    }
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean mapOps() {
        if(currentToken.word.equals(".")) {
            out += "\n     .";
            if(getNextToken()) {
                if(currentToken.word.equals("collect")) { // empty case
                    return true;
                } else if (currentToken.word.equals("map")) { // non-empty case
                    out += "selectExpr";
                    if(getNextToken()) {
                        if(currentToken.word.equals("(")) {
                            out += "(";
                            if(getNextToken()) {
                                if(UDF()) {
                                    // TODO
                                    out += SQL;
                                    // out += "<SQL>"; // temp placeholder
                                    // may need Stack or symbol table.
                                    if(currentToken.word.equals(")")) {
                                        out += ")";
                                        if(getNextToken()) {
                                            if(mapOps()) {
                                                return true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean UDF() {
        if(currentToken.type.equals("Identifier")) {
            // TODO
            UDFStack = new Stack<SimpleToken>();
            symbolTable = new HashMap<String,String>();
            SQL = "";
            symbolTable.put(currentToken.word, "Start");
            if(getNextToken()){
                if(currentToken.word.equals("=>")) {
                    if(getNextToken()) {
                        if(expression()) {
                            UDFSymbolTables.add(symbolTable);
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean expression() {
        if(currentToken.word.equals("{")){
            if(getNextToken()) {
                if (complexExpr()) {
                    if (currentToken.word.equals("}")) {
                        if(getNextToken()) {
                            return true;
                        }
                    }
                }
            }
        } else if(simpleExpr()) {
            return true;
        }
        return false;
    }

    private boolean simpleExpr() {
        SQL += "\"";
        if(currentToken.word.equals("(")) {
            if(getNextToken()) {
                if(tupleExpr()) {
                    if(currentToken.word.equals(")")) {
                        if(getNextToken()) {
                            return true;
                        }
                    }
                }
            }
        } else if (pureExpr()) {
            SQL += " as _1\"";
            return true;
        }
        return false;
    }

    private boolean tupleExpr() {
        if(pureExpr()) {
            tupleCount = 1;
            //if(getNextToken()) {
                if(currentToken.word.equals(",")) { // we have a tuple
                    SQL += " as _" + tupleCount + "\"";
                    SQL += ", \"";
                    if(getNextToken()) {
                        if (tupleExpr2()) {
                            return true;
                        }
                    }
                } else if(currentToken.word.equals(")")) { // we guessed wrong
                    // if(getNextToken()) {
                        SQL += " as _1\"";
                        return true;
                    // }
                }
            //}
        }
        System.out.print("\n\ntupleExpr() fails on " + currentToken.word);
        return false;
    }

    private boolean tupleExpr2() {
        if(pureExpr()) {
            tupleCount++;
            if(currentToken.word.equals(",")) {
                SQL += " as _" + tupleCount + "\"";
                SQL += ", \"";
                if(getNextToken()) {
                    if(tupleExpr2()) {
                        return true;
                    }
                }
            } else /*if(currentToken.word.equals(")"))*/ {
                // if(getNextToken()) {
                    SQL += " as _" + tupleCount + "\"";
                    return true;
                // }
            }
        }
        System.out.print("\n\ntupleExpr2() fails on " + currentToken.word);
        return false;
    }

    private boolean complexExpr() {
        if(currentToken.word.equals("val")) {
            if(assignExprs()) {
                // if(currentToken.word.equals(";")) { // consumed by assignExprs
                    // if(getNextToken()) {
                        if(simpleExpr()) {
                            return true;
                        }
                    // }
                // }
            }
        } else {
            if(simpleExpr()) {
                return true;
            }
        }
        return false;
    }

    private boolean assignExprs() {
        if(assignExpr()) {
            if(assignExprs2()) {
                return true;
            }
        }
        return false;
    }

    private boolean assignExprs2() {
        if(currentToken.word.equals(";")) {
            if(getNextToken()) {
                if (assignExprs()) {
                    return true;
                } else {
                    return true;
                }
            }
        } else {
            return true;
        }
        return false;
    }

    private boolean assignExpr() {
        if(currentToken.word.equals("val")) {
            inAssignExpr = true;
            if(getNextToken()) {
                if(currentToken.type.equals("Identifier")) {
                    String key = currentToken.word;
                    if(getNextToken()) {
                        if(currentToken.word.equals("=")) {
                            UDFStack.push(currentToken);
                            if(getNextToken()) {
                                if(pureExpr()) {
                                    String val = "";

                                    while(!UDFStack.peek().word.equals("=")) {
                                        SimpleToken token = UDFStack.pop();
                                        if(token.type.equals("Identifier") && !UDFStack.peek().word.equals(".")) {
                                            if(symbolTable.containsKey(token.word)) {
                                                if(symbolTable.get(token.word).equals("Start")) {
                                                    val = "_1" + val;
                                                }
                                            }
                                        } else {
                                            val = token.word + val;
                                        }
                                    }
                                    // if(!symbolTable.containsKey(key)) {
                                        symbolTable.put(key, val);
                                    // }
                                    inAssignExpr = false;
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean pureExpr() {
        if(currentToken.type.equals("Identifier") || currentToken.type.equals("Number")) {
            if(currentToken.type.equals("Number") && !inAssignExpr) {
                SQL += currentToken.word;
            }
            UDFStack.push(currentToken);
            if(getNextToken()) {
                if(pureExpr2()) {
                    if(pureExpr3()) {
                        return true;
                    }
                }
            }
        } else if (currentToken.word.equals("(")) {
            UDFStack.push(currentToken);
            if(!inAssignExpr) {
                SQL += "(";
            }
            if(getNextToken()) {
                if(pureExpr()) {
                    if(getNextToken()) {
                        if(currentToken.word.equals(")")) {
                            UDFStack.push(currentToken);
                            if(!inAssignExpr) {
                                SQL += ")";
                            }
                            if(getNextToken()) {
                                return true;
                            }
                        }
                    }
                }
            }
        } else if (currentToken.word.equals("if")) {
            UDFStack.push(currentToken);
            if(!inAssignExpr) {
                SQL += "if";
            }
            if(getNextToken()) {
                if(currentToken.word.equals("(")) {
                    UDFStack.push(currentToken);
                    if(!inAssignExpr) {
                        SQL += "(";
                    }
                    if(getNextToken()) {
                        if(compExpr()) {
                            if(currentToken.word.equals(")")) {
                                UDFStack.push(currentToken);
                                if(!inAssignExpr) {
                                    SQL += ",";
                                }
                                if(getNextToken()) {
                                    if(pureExpr()) {
                                        if(currentToken.word.equals("else")) {
                                            UDFStack.push(currentToken);
                                            if(!inAssignExpr) {
                                                SQL += ",";
                                            }
                                            if(getNextToken()) {
                                                if(pureExpr()) {
                                                    if(!inAssignExpr) {
                                                        SQL += ")";
                                                    }
                                                    return true;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean pureExpr2() {
        if(currentToken.word.equals(".")) {
            UDFStack.push(currentToken);
            if(getNextToken()) {
                if(currentToken.type.equals("Identifier") || currentToken.type.equals("Number")) {
                    UDFStack.push(currentToken);
                    if(currentToken.type.equals("Number")) {
                        System.err.println("\nError: Floating point number inside a UDF");
                        return false;
                    } else if(!inAssignExpr) {
                        UDFStack.pop();
                        UDFStack.pop();
                        String key = UDFStack.pop().word;
                        if(symbolTable.containsKey(key) && symbolTable.get(key).equals("Start")) {
                            SQL += currentToken.word;
                        } else {
                            System.err.println("\nError: Variable must be declared before use");
                            return false;
                        }
                    }
                    if(getNextToken()) {
                        return true;
                    }
                }
            }
        } else { // empty case
            if(!inAssignExpr) {
                // UDFStack.pop(); // pop off the .
                String ID = UDFStack.pop().word; // get the identifier
                if(symbolTable.containsKey(ID)) {
                    if(symbolTable.get(ID).equals("Start")) {
                        SQL += "_1";
                    } else {
                        SQL += symbolTable.get(ID);
                    }
                }
            }
            return true;
        }
        return false;
    }

    private boolean pureExpr3() {
        if(op()) {
            if(pureExpr()) {
                return true;
            }
        } else { // empty case
            return true;
        }
        return false;
    }

    private boolean compExpr() {
        if(pureExpr()) {
            if(comp()) {
                if(pureExpr()) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean op() {
        if(currentToken.word.equals("+") || currentToken.word.equals("-")
                || currentToken.word.equals("*") || currentToken.word.equals("%")) {
            UDFStack.push(currentToken);
            if(!inAssignExpr) {
                SQL += currentToken.word;
            }
            if(getNextToken()) {
                return true;
            }
        }
        return false;
    }

    private boolean comp() {
        if(currentToken.word.equals("==") || currentToken.word.equals("!=")
                || currentToken.word.equals("<") || currentToken.word.equals(">")
                || currentToken.word.equals("<=") || currentToken.word.equals(">=")) {
            UDFStack.push(currentToken);
            if(!inAssignExpr) {
                SQL += currentToken.word;
            }
            if(getNextToken()) {
                return true;
            }
        }
        return false;
    }

}
