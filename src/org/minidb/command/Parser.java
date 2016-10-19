package org.minidb.command;

import org.minidb.command.ddl.AlterTableAddConstraint;
import org.minidb.command.ddl.CreateIndex;
import org.minidb.command.ddl.CreateTable;
import org.minidb.command.ddl.CreateUser;
import org.minidb.command.dml.Insert;
import org.minidb.command.dml.Query;
import org.minidb.command.dml.Select;
import org.minidb.engine.Database;
import org.minidb.engine.DbException;
import org.minidb.expression.Comparison;
import org.minidb.expression.Expression;
import org.minidb.expression.ExpressionColumn;
import org.minidb.expression.ValueExpression;
import org.minidb.schema.Schema;
import org.minidb.engine.Session;
import org.minidb.table.IndexColumn;
import org.minidb.table.Table;
import org.minidb.table.TableFilter;
import org.minidb.util.DataType;
import org.minidb.table.Column;
import org.minidb.util.*;
import org.minidb.value.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;

/**
 * Created by gxh on 2016/6/10.
 */
public class Parser {
    // used during the tokenizer phase
    private static final int CHAR_END = 1, CHAR_VALUE = 2, CHAR_QUOTED = 3;
    private static final int CHAR_NAME = 4, CHAR_SPECIAL_1 = 5, CHAR_SPECIAL_2 = 6;
    private static final int CHAR_STRING = 7, CHAR_DOT = 8, CHAR_DOLLAR_QUOTED_STRING = 9;

    // this are token types
    private static final int KEYWORD = 1, IDENTIFIER = 2, PARAMETER = 3, END = 4, VALUE = 5;
    private static final int EQUAL = 6, BIGGER_EQUAL = 7, BIGGER = 8;
    private static final int SMALLER = 9, SMALLER_EQUAL = 10, NOT_EQUAL = 11, AT = 12;
    private static final int MINUS = 13, PLUS = 14, STRING_CONCAT = 15;
    private static final int OPEN = 16, CLOSE = 17, NULL = 18, TRUE = 19, FALSE = 20;
    private static final int CURRENT_TIMESTAMP = 21, CURRENT_DATE = 22, CURRENT_TIME = 23, ROWNUM = 24;
    private static final int SPATIAL_INTERSECTS = 25;

    private final Database database;
    private final Session session;

    /** indicates character-type for each char in sqlCommand */
    private int[] characterTypes;
    private int currentTokenType;
    private String currentToken;
    private boolean currentTokenQuoted;
    private Value currentValue;
    private String originalSQL;
    /** copy of originalSQL, with comments blanked out */
    private String sqlCommand;
    /** cached array if chars from sqlCommand */
    private char[] sqlCommandChars;
    /** index into sqlCommand of previous token */
    private int lastParseIndex;
    /** index into sqlCommand of current token */
    private int parseIndex;
    //    private CreateView createView;
    private String schemaName;
    private int orderInFrom;

    public Parser(Session session) {
        this.database = session.getDatabase();
        this.session = session;
    }

    public Command prepareCommand(String sql) {
        try {
            Prepared p = parse(sql);
            p.prepare();
            Command c = new Command(this, sql, p);
            return c;
        } catch (DbException e) {
            throw new RuntimeException("prepareCommand error");
        }
    }

    private Prepared parse(String sql) {
        initialize(sql);
        read();
        return parsePrepared();
    }

    public Prepared prepare(String sql) {
        Prepared p = parse(sql);
        p.prepare();
        if (currentTokenType != END) {
            throw new RuntimeException("prepare ERROR");
        }
        return p;
    }

    //对SQL中的每个字符标明其类型，以便下一步在read方法中识别sql中的各种结构。
    private void initialize(String sql) {
        if (sql == null) {
            sql = "";
        }
        originalSQL = sql;                  //不会变，最原始的SQL
        sqlCommand = sql;                   //会变
        int len = sql.length() + 1;
        char[] command = new char[len];     //command数组存放sql转换成的字符
        int[] types = new int[len];         //types数组存放每个token的类型
        len--;
        sql.getChars(0, len, command, 0);   //sql字符串转换为char数组
        boolean changed = false;
        command[len] = ' ';                 //最后一位加个空格

        for (int i = 0; i < len; i++) {
            char c = command[i];
            int type = 0;
            switch (c) {
                case '(':
                case ')':
                case '{':
                case '}':
                case '*':
                case ',':
                case ';':
                case '+':
                case '%':
                case '?':
                case '@':
                case ']':
                    type = CHAR_SPECIAL_1;
                    break;
                case '!':
                case '<':
                case '>':
                case '|':
                case '=':
                case ':':
                case '&':
                case '~':
                    type = CHAR_SPECIAL_2; //这类字符可两两组合，如"<="、"!="、"<>“
                    break;
                case '.':
                    type = CHAR_DOT;
                    break;
                case '\'': //字符串，注意在sql里字符串是用单引号括起来，不像java是用双引号
                    type = types[i] = CHAR_STRING;
                    break;
                case '_':
                    type = CHAR_NAME;
                    break;
                default:
                    if (c >= 'a' && c <= 'z') {
                        command[i] = (char) (c - ('a' - 'A'));   //a-z要转成大写
                        changed = true;
                        type = CHAR_NAME;
                    } else if (c >= 'A' && c <= 'Z') {
                        type = CHAR_NAME;
                    } else if (c >= '0' && c <= '9') {
                        type = CHAR_VALUE;
                    } else {
                        if (c <= ' ' || Character.isSpaceChar(c)) { //控制字符和空白对应的type都是0
                            // whitespace
                        }
                    }
            }
            types[i] = type;
        }
        sqlCommandChars = command;            //sqlCommandChars存放变成大写以后的sql的字符数组的形式
        types[len] = CHAR_END;                //types数组最后一位放结束标志
        characterTypes = types;               //characterTypes 存放 type
        if (changed) {
            sqlCommand = new String(command);
        }
        parseIndex = 0;
    }

    private void read() {
        int[] types = characterTypes;
        lastParseIndex = parseIndex;
        int i = parseIndex;
        int type = types[i];
        while (type == 0) {     //跳过最前面type为0的元素，因为0对应的字符是空白类的
            type = types[++i];
        }
        int start = i;
        char[] chars = sqlCommandChars;
        char c = chars[i++]; //注意这里，c是当前字符，当下面chars[i]时就是下一个字符了
        currentToken = "";
        switch (type) {
            case CHAR_NAME:
                while (true) {             //找到下一个不为字符和数字的
                    type = types[i];
                    if (type != CHAR_NAME && type != CHAR_VALUE) {
                        break;
                    }
                    i++;
                }
                currentToken = sqlCommand.substring(start, i);   //提取出这个token
                currentTokenType = getTokenType(currentToken);   //获取token的类型
                parseIndex = i;                                  //记录当前解析到的位置
                return;
            case CHAR_SPECIAL_2:
                //两个CHAR_SPECIAL_2类型的字符要合并。例如!=
                if (types[i] == CHAR_SPECIAL_2) {
                    i++;
                }
                currentToken = sqlCommand.substring(start, i);
                currentTokenType = getSpecialType(currentToken);
                parseIndex = i;
                return;
            case CHAR_SPECIAL_1:
                currentToken = sqlCommand.substring(start, i);
                currentTokenType = getSpecialType(currentToken);
                parseIndex = i;
                return;
            case CHAR_VALUE:
                //如果DATABASE_TO_UPPER是false，那么0x是错的
                //如sql = "select id,name from ParserTest where id > 0x2";
                //只能用大写0X，并且也只能用A-F，
                //否则像where id > 0X2ab，实际是where id > 0X2，但是ab没有读到，
                //当判断org.h2.command.Parser.prepareCommand(String)时，(currentTokenType != END)为false就出错
                if (c == '0' && chars[i] == 'X') { //在initialize中已把x转换成大写X
                    // hex number
                    long number = 0;
                    start += 2;
                    i++;
                    while (true) {
                        c = chars[i];
                        if ((c < '0' || c > '9') && (c < 'A' || c > 'F')) {
                            //checkLiterals(false);
                            currentValue = ValueInt.get((int) number);
                            currentTokenType = VALUE;
                            currentToken = "0";
                            parseIndex = i;
                            return;
                        }
                        //(number << 4)表示乘以16,而"c - (c >= 'A' ? ('A' - 0xa) : ('0')"是算当前c与'0'或‘A'的差值
                        //如果c>='A'，那么c = 0xa+(c-'A')
                        //如果c>='0',且小于'A'，那么c = '0'+(c-'0');
                        number = (number << 4) + c - (c >= 'A' ? ('A' - 0xa) : ('0'));
                        //16进制值>Integer.MAX_VALUE时转成BigDecimal来表示
//                        if (number > Integer.MAX_VALUE) {
//                            readHexDecimal(start, i);
//                            return;
//                        }
                        i++;
                    }
                }
                long number = c - '0';
                while (true) {
                    c = chars[i];
                    if (c < '0' || c > '9') {
//                        if (c == '.' || c == 'E' || c == 'L') {
//                            readDecimal(start, i);
//                            break;
//                        }
                        //checkLiterals(false);
                        currentValue = ValueInt.get((int) number);
                        currentTokenType = VALUE;
                        currentToken = "0";
                        parseIndex = i;
                        break;
                    }
                    number = number * 10 + (c - '0');
//                    if (number > Integer.MAX_VALUE) {
//                        readDecimal(start, i);
//                        break;
//                    }
                    i++;
                }
                return;
            case CHAR_DOT:
                if (types[i] != CHAR_VALUE) {
                    currentTokenType = KEYWORD;
                    currentToken = ".";
                    parseIndex = i;
                    return;
                }
                readDecimal(i - 1, i); //如".123"时，因为c是点号，c对应i-1，所以要把c包含进来
                return;
            case CHAR_STRING: { //字符串Literal
                String result = null;
                //与CHAR_QUOTED类似
                while (true) {
                    for (int begin = i;; i++) {
                        if (chars[i] == '\'') {
                            if (result == null) {
                                result = sqlCommand.substring(begin, i);
                            } else {
                                result += sqlCommand.substring(begin - 1, i);
                            }
                            break;
                        }
                    }
                    if (chars[++i] != '\'') {
                        break;
                    }
                    i++;
                }
                currentToken = "'";
                //checkLiterals(true);
                currentValue = ValueString.get(result);
                parseIndex = i;
                currentTokenType = VALUE;
                return;
            }
            case CHAR_END:
                currentToken = "";
                currentTokenType = END;
                parseIndex = i;
                return;
            default:
                //throw getSyntaxError();
        }
    }

    private Prepared parsePrepared() {
        int start = lastParseIndex;
        Prepared c = null;
        String token = currentToken;
        char first = token.charAt(0);
        switch (first) {
            case 'c':
            case 'C':
                if (readIf("CREATE"))
                    c = parseCreate();
                break;
            case 'i':
            case 'I':
                if (readIf("INSERT")) {
                    c = parseInsert();
                }
                break;
            case 's':
            case 'S':
                if (isToken("SELECT")) {
                    c = parseSelect();
                }
//                else if (readIf("SET")) {
//                    c = parseSet();
//                }
//                else if (readIf("SHOW")) {
//                    c = parseShow();
//                }
                break;
            case ';':
                //c = new NoOperation(session);
                break;
            default:
                throw new RuntimeException("parsePrepared ERROR");
                //throw getSyntaxError();
        }
        setSQL(c, null, start);
        return c;
    }

    private Insert parseInsert() {
        Insert command = new Insert(session);
        read("INTO");
        Table table = readTableOrView();
        command.setTable(table);
        Column[] columns = null;
        if (readIf("DEFAULT")) {
            read("VALUES");
            Expression[] expr = {};
            command.addRow(expr);
        } else if (readIf("VALUES")) {
            read("(");
            do {
                ArrayList<Expression> values = new ArrayList();
                if (!readIf(")")) {
                    do {
                        if (readIf("DEFAULT")) {
                            values.add(null);
                        } else {
                            values.add(readExpression());
                        }
                    } while (readIfMore());
                }
                command.addRow(values.toArray(new Expression[values.size()]));
                // the following condition will allow (..),; and (..);
            } while (readIf(",") && readIf("("));
        } else if (readIf("SET")) {
            if (columns != null) {
                throw new RuntimeException("parseInsert ERROR");
            }
            ArrayList<Column> columnList = new ArrayList();
            ArrayList<Expression> values = new ArrayList();
            do {
                columnList.add(parseColumn(table));
                read("=");
                Expression expression;
                if (readIf("DEFAULT")) {
                    expression = ValueExpression.getDefault();
                } else {
                    expression = readExpression();
                }
                values.add(expression);
            } while (readIf(","));
            command.setColumns(columnList.toArray(new Column[columnList.size()]));
            command.addRow(values.toArray(new Expression[values.size()]));
        } else {
            command.setQuery(parseSelect());
        }
        return command;
    }

    private Column parseColumn(Table table) {
        String id = readColumnIdentifier();
        return table.getColumn(id);
    }

    private Table readTableOrView() {
        return readTableOrView(readIdentifierWithSchema(null));
    }

    private Query parseSelect() {
        Query command = parseSelectUnion();
        command.init();
        return command;
    }

    private Query parseSelectUnion() {
        int start = lastParseIndex;
        Query command = parseSelectSub();
        return parseSelectUnionExtension(command, start, false);
    }

    private Query parseSelectSub() {
        Select select = parseSelectSimple();
        return select;
    }
    private Query parseSelectUnionExtension(Query command, int start,
                                            boolean unionOnly) {
        if (!unionOnly) {
//            parseEndOfQuery(command);
        }
        setSQL(command, null, start);
        return command;
    }


    private Select parseSelectSimple() {
        readIf("SELECT");

        Select command = new Select(session);
        int start = lastParseIndex;
        parseSelectSimpleSelectPart(command);
        readIf("FROM");
        parseSelectSimpleFromPart(command);
        if (readIf("WHERE")) {
            Expression condition = readExpression();
            command.addCondition(condition);
        }
        setSQL(command, "SELECT", start);
        return command;
    }

    private void parseSelectSimpleSelectPart(Select command) {
        ArrayList<Expression> expressions = new ArrayList();
        do {
            Expression expr = readExpression();
            expressions.add(expr);
        } while (readIf(","));
        command.setExpressions(expressions);
    }

    private void parseSelectSimpleFromPart(Select command) {
        do {
            TableFilter filter = readTableFilter(false);
            command.setTableFilter(filter);
        } while (readIf(","));
    }

    private TableFilter readTableFilter(boolean fromOuter) {
        Table table;
        String tableName = readIdentifierWithSchema(null);
        table = readTableOrView(tableName);
        return new TableFilter(session, table);
    }

    private Table readTableOrView(String tableName) {
        // same algorithm than readSequence
        if (schemaName != null) {
            return getSchema().getTableOrView(session, tableName);
        }
        Table table = database.getSchema(session.getCurrentSchemaName())
                .findTableOrView(session, tableName);
        if (table != null) {
            return table;
        }
        throw new RuntimeException("readTableOrView ERROR");
    }

    private void setSQL(Prepared command, String start, int startIndex) {
        String sql = originalSQL.substring(startIndex, lastParseIndex).trim();
        if (start != null) {
            sql = start + " " + sql;
        }
        command.setSQL(sql);
    }

    private boolean isToken(String token) {
        boolean result = equalsToken(token, currentToken) &&
                !currentTokenQuoted;
        if (result) {
            return true;
        }
//        addExpected(token);
        return false;
    }

    private boolean readIf(String token) {
        if (equalsToken(token, currentToken)) {
            read();
            return true;
        }
//        addExpected(token);
        return false;
    }

    private boolean equalsToken(String a, String b) {
        if (a == null) {
            return b == null;
        } else if (a.equals(b)) {
            return true;
        }
        return false;
    }

    private void readDecimal(int start, int i) {
        char[] chars = sqlCommandChars;
        int[] types = characterTypes;
        // go until the first non-number
        while (true) {
            int t = types[i]; //刚开始时types[i是CHAR_DOT
            //比如要找e/E或其他字符比如空格啊，说明数字结束了
            if (t != CHAR_DOT && t != CHAR_VALUE) {
                break;
            }
            i++;
        }
        boolean containsE = false;
        if (chars[i] == 'E' || chars[i] == 'e') {
            containsE = true;
            i++;
            if (chars[i] == '+' || chars[i] == '-') {
                i++;
            }
            if (types[i] != CHAR_VALUE) {
                //throw getSyntaxError();
            }
            while (types[++i] == CHAR_VALUE) {
                // go until the first non-number
            }
        }
        parseIndex = i;
        String sub = sqlCommand.substring(start, i);
        //checkLiterals(false);
        if (!containsE && sub.indexOf('.') < 0) {
            BigInteger bi = new BigInteger(sub);
            if (bi.compareTo(ValueLong.MAX) <= 0) {
                // parse constants like "10000000L"
                if (chars[i] == 'L') {
                    parseIndex++;
                }
                currentValue = ValueLong.get(bi.longValue());
                currentTokenType = VALUE;
                return;
            }
        }
        BigDecimal bd = null;
        try {
            bd = new BigDecimal(sub);
        } catch (NumberFormatException e) {
            //throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, sub);
        }
        currentValue = ValueDecimal.get(bd);
        currentTokenType = VALUE;
    }

    private int getSpecialType(String s) {
        char c0 = s.charAt(0);
        if (s.length() == 1) {
            switch (c0) {
                case '?':
                case '$':
                    return PARAMETER;
                case '@':
                    return AT;
                case '+':
                    return PLUS;
                case '-':
                    return MINUS;
                case '{':
                case '}':
                case '*':
                case '/':
                case '%':
                case ';':
                case ',':
                case ':':
                case '[':
                case ']':
                case '~':
                    return KEYWORD;
                case '(':
                    return OPEN;
                case ')':
                    return CLOSE;
                case '<':
                    return SMALLER;
                case '>':
                    return BIGGER;
                case '=':
                    return EQUAL;
                default:
                    break;
            }
        } else if (s.length() == 2) {
            switch (c0) {
                case ':':
                    if ("::".equals(s)) {
                        return KEYWORD;
                    } else if (":=".equals(s)) {
                        return KEYWORD;
                    }
                    break;
                case '>':
                    if (">=".equals(s)) {
                        return BIGGER_EQUAL;
                    }
                    break;
                case '<':
                    if ("<=".equals(s)) {
                        return SMALLER_EQUAL;
                    } else if ("<>".equals(s)) {
                        return NOT_EQUAL;
                    }
                    break;
                case '!':
                    if ("!=".equals(s)) {
                        return NOT_EQUAL;
                    } else if ("!~".equals(s)) {
                        return KEYWORD;
                    }
                    break;
                case '|':
                    if ("||".equals(s)) {
                        return STRING_CONCAT;
                    }
                    break;
                case '&':
                    if ("&&".equals(s)) {
                        return SPATIAL_INTERSECTS;
                    }
                    break;
            }
        }
        //throw getSyntaxError();
        return 0;
    }

    private int getTokenType(String s) {
        switch (s.charAt(0)) {
            case 'C':
                if (s.equals("CURRENT_TIMESTAMP")) {
                    return CURRENT_TIMESTAMP;
                } else if (s.equals("CURRENT_TIME")) {
                    return CURRENT_TIME;
                } else if (s.equals("CURRENT_DATE")) {
                    return CURRENT_DATE;
                }
                return getKeywordOrIdentifier(s, "CROSS", KEYWORD);
            case 'D':
                return getKeywordOrIdentifier(s, "DISTINCT", KEYWORD);
            case 'E':
                if ("EXCEPT".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "EXISTS", KEYWORD);
            case 'F':
                if ("FROM".equals(s)) {
                    return KEYWORD;
                } else if ("FOR".equals(s)) {
                    return KEYWORD;
                } else if ("FULL".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "FALSE", FALSE);
            case 'G':
                return getKeywordOrIdentifier(s, "GROUP", KEYWORD);
            case 'H':
                return getKeywordOrIdentifier(s, "HAVING", KEYWORD);
            case 'I':
                if ("INNER".equals(s)) {
                    return KEYWORD;
                } else if ("INTERSECT".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "IS", KEYWORD);
            case 'J':
                return getKeywordOrIdentifier(s, "JOIN", KEYWORD);
            case 'L':
                if ("LIMIT".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "LIKE", KEYWORD);
            case 'M':
                return getKeywordOrIdentifier(s, "MINUS", KEYWORD);
            case 'N':
                if ("NOT".equals(s)) {
                    return KEYWORD;
                } else if ("NATURAL".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "NULL", NULL);
            case 'O':
                if ("ON".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "ORDER", KEYWORD);
            case 'P':
                return getKeywordOrIdentifier(s, "PRIMARY", KEYWORD);
            case 'S':
                if (s.equals("SYSTIMESTAMP")) {
                    return CURRENT_TIMESTAMP;
                } else if (s.equals("SYSTIME")) {
                    return CURRENT_TIME;
                } else if (s.equals("SYSDATE")) {
                    return CURRENT_TIMESTAMP;
                }
                return getKeywordOrIdentifier(s, "SELECT", KEYWORD);
            case 'T':
                if ("TODAY".equals(s)) {
                    return CURRENT_DATE;
                }
                return getKeywordOrIdentifier(s, "TRUE", TRUE);
            case 'U':
                if ("UNIQUE".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "UNION", KEYWORD);
            case 'W':
                if ("WITH".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "WHERE", KEYWORD);
            default:
                return IDENTIFIER;
        }
    }


    private static int getKeywordOrIdentifier(String s1, String s2, int keywordType) {
        if (s1.equals(s2)) {
            return keywordType;
        }
        return IDENTIFIER;
    }

    private Prepared parseCreate() {
        if (readIf("TABLE")) {
            return parseCreateTable();
        }else if (readIf("USER")) {
            return parseCreateUser();
        }else {
            boolean hash = false, primaryKey = false;
            boolean unique = false, spatial = false;
            String indexName = null;
            Schema oldSchema = null;
            boolean ifNotExists = false;
            if (readIf("PRIMARY")) {
                read("KEY");
                primaryKey = true;
                if (!isToken("ON")) {
                    ifNotExists = readIfNotExists();
                    indexName = readIdentifierWithSchema(null);
                    oldSchema = getSchema();
                }
            }
            read("ON");
            String tableName = readIdentifierWithSchema();
            CreateIndex command = new CreateIndex(session, getSchema());
            command.setIfNotExists(ifNotExists);
            command.setPrimaryKey(primaryKey);
            command.setTableName(tableName);
            command.setUnique(unique);
            command.setIndexName(indexName);
            read("(");
            command.setIndexColumns(parseIndexColumnList());
            return command;
        }
    }

    private IndexColumn[] parseIndexColumnList() {
        ArrayList<IndexColumn> columns = new ArrayList();
        do {
            IndexColumn column = new IndexColumn();
            column.columnName = readColumnIdentifier();
            columns.add(column);
        } while (readIf(","));
        read(")");
        return columns.toArray(new IndexColumn[columns.size()]);
    }

    private CreateUser parseCreateUser() {
        CreateUser command = new CreateUser(session);
        command.setIfNotExists(readIfNotExists());
        command.setUserName(readUniqueIdentifier());
        if (readIf("PASSWORD")) {
            command.setPassword(readExpression());
        } else if (readIf("SALT")) {
            command.setSalt(readExpression());
            read("HASH");
            command.setHash(readExpression());
        } else {
            throw new RuntimeException("parseCreateUser ERROR");
        }
        if (readIf("ADMIN")) {
            command.setAdmin(true);
        }
        return command;
    }

    private boolean readIfNotExists() {
        if (readIf("IF")) {
            read("NOT");
            read("EXISTS");
            return true;
        }
        return false;
    }

/*---------------------------------------------------*/
    private Expression readExpression() {
        Expression r = readAnd();
        return r;
    }

    private Expression readAnd() {
        Expression r = readCondition();
        return r;
    }

    private Expression readCondition() {
        Expression r = readConcat();
        while (true) {
            int compareType = getCompareType(currentTokenType);
            if (compareType < 0) {
                break;
            }
            read();
            Expression right = readConcat();
            r = new Comparison(session, compareType, r, right);
        }
        return r;
    }

    private static int getCompareType(int tokenType) {
        switch (tokenType) {
            case EQUAL:
                return Comparison.EQUAL;
            case BIGGER_EQUAL:
                return Comparison.BIGGER_EQUAL;
            case BIGGER:
                return Comparison.BIGGER;
            case SMALLER:
                return Comparison.SMALLER;
            case SMALLER_EQUAL:
                return Comparison.SMALLER_EQUAL;
            case NOT_EQUAL:
                return Comparison.NOT_EQUAL;
            case SPATIAL_INTERSECTS:
                return Comparison.SPATIAL_INTERSECTS;
            default:
                return -1;
        }
    }

    private Expression readConcat() {
        Expression r = readSum();
        return r;
    }

    private Expression readSum() {
        Expression r = readFactor();
        return r;
    }

    private Expression readFactor() {
        Expression r = readTerm();
        return r;
    }

    private Expression readTerm() {
        Expression r;
        switch (currentTokenType) {
            case IDENTIFIER:
                String name = currentToken;
                read();
                r = new ExpressionColumn(database, null, null, name);
                break;
            case VALUE:
                r = ValueExpression.get(currentValue);
                read();
                break;
            default:
                throw new RuntimeException("readTerm ERROR");
        }
        return r;
    }
/*---------------------------------------------------*/

    private String readUniqueIdentifier() {
        return readColumnIdentifier();
    }

    private String readColumnIdentifier() {
        if (currentTokenType != IDENTIFIER) {
            throw new RuntimeException("readColumnIdentifier ERROR");
        }
        String s = currentToken;
        read();
        return s;
    }

    private String readIdentifierWithSchema(String defaultSchemaName) {
        String s = currentToken;
        read();
        schemaName = defaultSchemaName;
        if (readIf(".")) {
            schemaName = s;
            s = currentToken; //public
            read();
        }
        if (equalsToken(".", currentToken)) {
            if (equalsToken(schemaName, database.getDatabaseShortName())) {
                read(".");
                schemaName = s;
                s = currentToken;
                read();
            }
        }
        return s;
    }

    private String readIdentifierWithSchema() {
        return readIdentifierWithSchema(session.getCurrentSchemaName());
    }

    private Schema getSchema(String schemaName) {
        if (schemaName == null) {
            return null;
        }
        Schema schema = database.findSchema(schemaName);
//        if (schema == null) {
//            if (equalsToken("SESSION", schemaName)) {
//                // for local temporary tables
//                schema = database.getSchema(session.getCurrentSchemaName());
//            }
//        }
        return schema;
    }

    private Schema getSchema() {
        return getSchema(schemaName);
    }

    private CreateTable parseCreateTable() {
        String tableName = readIdentifierWithSchema();
        Schema schema = getSchema();
        CreateTable command = new CreateTable(session, schema);
        command.setTableName(tableName);

        if (readIf("(")) { //可以没有列
            if (!readIf(")")) {
                do {
                    String columnName = readColumnIdentifier();
                    Column column = parseColumnForTable(columnName, true);
                    command.addColumn(column);
                    if (readIf("PRIMARY")) {
                        read("KEY");
                        boolean hash = readIf("HASH");
                        IndexColumn[] cols = { new IndexColumn() };
                        cols[0].columnName = column.getName();
                        AlterTableAddConstraint pk = new AlterTableAddConstraint(
                                session, schema, false);
                        pk.setType(CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY);
                        pk.setTableName(tableName);
                        pk.setIndexColumns(cols);
                        command.addConstraintCommand(pk);
                    }
                } while (readIfMore());
            }
        }
        return command;
    }

//    private String readColumnIdentifier() {
//        if (currentTokenType != IDENTIFIER) {
//            //throw DbException.getSyntaxError(sqlCommand, parseIndex,
//            //        "identifier");
//        }
//        String s = currentToken;
//        read();
//        return s;
//    }

    private Column parseColumnForTable(String columnName, boolean defaultNullable) {
        Column column;
        column = parseColumnWithType(columnName); //解析列类型
        column.setNullable(defaultNullable & column.isNullable());
        return column;
    }

    private Column parseColumnWithType(String columnName) {
        String original = currentToken; //字段类型
        DataType dataType = DataType.getTypeByName(original);
        read();
        if (readIf("(")) {    //varchar(20) 这种先忽略
            readLong();
            read(")");
        }
        int type = dataType.type;
        Column column = new Column(columnName, type);
        column.setOriginalSQL(original);
        return column;
    }

    private void read(String expected) {
        //用`、[]、“包围起来的字符串所代表的Token会使得currentTokenQuoted=true
        //如"CREATE or `REPLACE` TABLE IF NOT EXISTS
        //expected = currentToken = REPLACE
        //currentTokenQuoted = true
        if (currentTokenQuoted || !equalsToken(expected, currentToken)) {
//            addExpected(expected);
            //throw getSyntaxError();
        }
        read();
    }

    private int readInt() {
        boolean minus = false;
        if (currentTokenType == MINUS) {
            minus = true;
            read();
        } else if (currentTokenType == PLUS) {
            read();
        }
        if (currentTokenType != VALUE) {
            //throw DbException.getSyntaxError(sqlCommand, parseIndex, "integer");
        }
        if (minus) {
            // must do that now, otherwise Integer.MIN_VALUE would not work
            //currentValue = currentValue.negate();
        }
        int i = currentValue.getInt();
        read();
        return i;
    }

    private long readLong() {
        boolean minus = false;
        if (currentTokenType == MINUS) {
            minus = true;
            read();
        } else if (currentTokenType == PLUS) {
            read();
        }
        if (currentTokenType != VALUE) {
            //throw DbException.getSyntaxError(sqlCommand, parseIndex, "long");
        }
        if (minus) {
            // must do that now, otherwise Long.MIN_VALUE would not work
            //currentValue = currentValue.negate();
        }
        long i = currentValue.getLong();
        read();
        return i;
    }

    private boolean readIfMore() {
        if (readIf(",")) {
            return !readIf(")");
        }
        read(")");
        return false;
    }


    public Session getSession() {
        return session;
    }

    public static String quoteIdentifier(String s) {
        if (s == null || s.length() == 0) {
            return "\"\"";
        }
        char c = s.charAt(0);
        // lowercase a-z is quoted as well
        if ((!Character.isLetter(c) && c != '_') || Character.isLowerCase(c)) {
            return StringUtils.quoteIdentifier(s);
        }
        for (int i = 1, length = s.length(); i < length; i++) {
            c = s.charAt(i);
            if ((!Character.isLetterOrDigit(c) && c != '_') ||
                    Character.isLowerCase(c)) {
                return StringUtils.quoteIdentifier(s);
            }
        }
        if (isKeyword(s, true)) {
            return StringUtils.quoteIdentifier(s);
        }
        return s;
    }

    public static boolean isKeyword(String s, boolean supportOffsetFetch) {
        if (s == null || s.length() == 0) {
            return false;
        }
        return getSaveTokenType(s, supportOffsetFetch) != IDENTIFIER;
    }

    private static int getSaveTokenType(String s, boolean supportOffsetFetch) {
        switch (s.charAt(0)) {
            case 'C':
                if (s.equals("CURRENT_TIMESTAMP")) {
                    return CURRENT_TIMESTAMP;
                } else if (s.equals("CURRENT_TIME")) {
                    return CURRENT_TIME;
                } else if (s.equals("CURRENT_DATE")) {
                    return CURRENT_DATE;
                }
                return getKeywordOrIdentifier(s, "CROSS", KEYWORD);
            case 'D':
                return getKeywordOrIdentifier(s, "DISTINCT", KEYWORD);
            case 'E':
                if ("EXCEPT".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "EXISTS", KEYWORD);
            case 'F':
                if ("FROM".equals(s)) {
                    return KEYWORD;
                } else if ("FOR".equals(s)) {
                    return KEYWORD;
                } else if ("FULL".equals(s)) {
                    return KEYWORD;
                } else if (supportOffsetFetch && "FETCH".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "FALSE", FALSE);
            case 'G':
                return getKeywordOrIdentifier(s, "GROUP", KEYWORD);
            case 'H':
                return getKeywordOrIdentifier(s, "HAVING", KEYWORD);
            case 'I':
                if ("INNER".equals(s)) {
                    return KEYWORD;
                } else if ("INTERSECT".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "IS", KEYWORD);
            case 'J':
                return getKeywordOrIdentifier(s, "JOIN", KEYWORD);
            case 'L':
                if ("LIMIT".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "LIKE", KEYWORD);
            case 'M':
                return getKeywordOrIdentifier(s, "MINUS", KEYWORD);
            case 'N':
                if ("NOT".equals(s)) {
                    return KEYWORD;
                } else if ("NATURAL".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "NULL", NULL);
            case 'O':
                if ("ON".equals(s)) {
                    return KEYWORD;
                } else if (supportOffsetFetch && "OFFSET".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "ORDER", KEYWORD);
            case 'P':
                return getKeywordOrIdentifier(s, "PRIMARY", KEYWORD);
            case 'R':
                return getKeywordOrIdentifier(s, "ROWNUM", ROWNUM);
            case 'S':
                if (s.equals("SYSTIMESTAMP")) {
                    return CURRENT_TIMESTAMP;
                } else if (s.equals("SYSTIME")) {
                    return CURRENT_TIME;
                } else if (s.equals("SYSDATE")) {
                    return CURRENT_TIMESTAMP;
                }
                return getKeywordOrIdentifier(s, "SELECT", KEYWORD);
            case 'T':
                if ("TODAY".equals(s)) {
                    return CURRENT_DATE;
                }
                return getKeywordOrIdentifier(s, "TRUE", TRUE);
            case 'U':
                if ("UNIQUE".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "UNION", KEYWORD);
            case 'W':
                if ("WITH".equals(s)) {
                    return KEYWORD;
                }
                return getKeywordOrIdentifier(s, "WHERE", KEYWORD);
            default:
                return IDENTIFIER;
        }
    }

}

