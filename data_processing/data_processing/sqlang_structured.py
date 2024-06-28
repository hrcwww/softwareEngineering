# -*- coding: utf-8 -*-
import re
import sqlparse  # 导入sqlparse模块用于SQL解析

# 导入驼峰命名法模块
import inflection

# 导入词性标注和词形还原模块
from nltk import pos_tag
from nltk.stem import WordNetLemmati
wnler = WordNetLemmatizer()

#词干提取
from nltk.corpus import wordnet

#############################################################################
OTHER = 0
FUNCTION = 1
BLANK = 2
KEYWORD = 3
INTERNAL = 4

TABLE = 5
COLUMN = 6
INTEGER = 7
FLOAT = 8
HEX = 9
STRING = 10
WILDCARD = 11

SUBQUERY = 12

DUD = 13

# 将数字常量映射到它们的字符串表示的令牌类型字典

ttypes = {0: "OTHER", 1: "FUNCTION", 2: "BLANK", 3: "KEYWORD", 4: "INTERNAL", 5: "TABLE", 6: "COLUMN", 7: "INTEGER",
          8: "FLOAT", 9: "HEX", 10: "STRING", 11: "WILDCARD", 12: "SUBQUERY", 13: "DUD", }

# 正则表达式扫描器用于标记化
scanner = re.Scanner([(r"\[[^\]]*\]", lambda scanner, token: token), (r"\+", lambda scanner, token: "REGPLU"),
                      (r"\*", lambda scanner, token: "REGAST"), (r"%", lambda scanner, token: "REGCOL"),
                      (r"\^", lambda scanner, token: "REGSTA"), (r"\$", lambda scanner, token: "REGEND"),
                      (r"\?", lambda scanner, token: "REGQUE"),
                      (r"[\.~``;_a-zA-Z0-9\s=:\{\}\-\\]+", lambda scanner, token: "REFRE"),
                      (r'.', lambda scanner, token: None), ])

#---------------------子函数1：代码的规则--------------------

# 使用上述定义的扫描器对输入字符串`s`进行标记化
def tokenizeRegex(s):
    results = scanner.scan(s)[0]
    return results
    """
    使用预定义的正则表达式模式对输入字符串`s`进行标记化。

    参数:
    - s (str): 要标记化的输入字符串。

    返回:
    - list: 从`s`中提取的令牌列表。
    """


#---------------------子函数2：代码的规则--------------------
# SQL语言解析器类
class SqlangParser():
    # 通过确保以分号结尾并规范化括号来消除SQL字符串中的错误
    @staticmethod

    """
            通过规范化其格式来消除输入SQL字符串中的错误。

            参数:
            - sql (str): 输入的SQL字符串。

            返回:
            - str: 规范化后的SQL字符串。
    """
    def sanitizeSql(sql):
        s = sql.strip().lower()
        if not s[-1] == ";":
            s += ';'
        s = re.sub(r'\(', r' ( ', s)
        s = re.sub(r'\)', r' ) ', s)
        words = ['index', 'table', 'day', 'year', 'user', 'text']
        for word in words:
            s = re.sub(r'([^\w])' + word + '$', r'\1' + word + '1', s)
            s = re.sub(r'([^\w])' + word + r'([^\w])', r'\1' + word + '1' + r'\2', s)
        s = s.replace('#', '')
        return s

    """
           解析提供的令牌列表`tok`中的字符串标记并相应地修改它们。

           参数:
           - tok (sqlparse.sql.TokenList or sqlparse.sql.Token): 输入的令牌或令牌列表。

           返回:
           - None
    """
    # 解析和可能修改在提供的令牌列表`tok`中的字符串标记
    def parseStrings(self, tok):
        if isinstance(tok, sqlparse.sql.TokenList):
            for c in tok.tokens:
                self.parseStrings(c)
        elif tok.ttype == STRING:
            if self.regex:
                tok.value = ' '.join(tokenizeRegex(tok.value))
            else:
                tok.value = "CODSTR"

    """
           重命名提供的令牌列表`tok`中的标识符（列和表）。

           参数:
           - tok (sqlparse.sql.TokenList or sqlparse.sql.Token): 输入的令牌或令牌列表。

           返回:
           - None
    """
    def renameIdentifiers(self, tok):
        if isinstance(tok, sqlparse.sql.TokenList):
            for c in tok.tokens:
                self.renameIdentifiers(c)
        elif tok.ttype == COLUMN:
            if str(tok) not in self.idMap["COLUMN"]:
                colname = "col" + str(self.idCount["COLUMN"])
                self.idMap["COLUMN"][str(tok)] = colname
                self.idMapInv[colname] = str(tok)
                self.idCount["COLUMN"] += 1
            tok.value = self.idMap["COLUMN"][str(tok)]
        elif tok.ttype == TABLE:
            if str(tok) not in self.idMap["TABLE"]:
                tabname = "tab" + str(self.idCount["TABLE"])
                self.idMap["TABLE"][str(tok)] = tabname
                self.idMapInv[tabname] = str(tok)
                self.idCount["TABLE"] += 1
            tok.value = self.idMap["TABLE"][str(tok)]

        elif tok.ttype == FLOAT:
            tok.value = "CODFLO"
        elif tok.ttype == INTEGER:
            tok.value = "CODINT"
        elif tok.ttype == HEX:
            tok.value = "CODHEX"

    # 将 tokensWithBlanks 转换为元组并计算其哈希值作为对象的哈希值
    def __hash__(self):
        return hash(tuple([str(x) for x in self.tokensWithBlanks]))

    # 初始化 SqlangParser 实例，接收 SQL 查询语句和一些可选参数
    # 清理并标准化输入的 SQL 查询语句
    def __init__(self, sql, regex=False, rename=True):

        self.sql = SqlangParser.sanitizeSql(sql)    # 初始化映射和计数器用于识别列和表的 ID

        self.idMap = {"COLUMN": {}, "TABLE": {}}
        self.idMapInv = {}
        self.idCount = {"COLUMN": 0, "TABLE": 0}
        self.regex = regex                          # 是否启用正则表达式的标志

        self.parseTreeSentinel = False              # 解析树的哨兵标志和表堆栈
        self.tableStack = []

        self.parse = sqlparse.parse(self.sql)       # 解析 SQL 查询语句并处理
        self.parse = [self.parse[0]]

        self.removeWhitespaces(self.parse[0])       # 去除空白符
        self.identifyLiterals(self.parse[0])        # 识别字面量
        self.parse[0].ptype = SUBQUERY              # 设置子查询标志
        self.identifySubQueries(self.parse[0])
        self.identifyFunctions(self.parse[0])       # 识别函数
        self.identifyTables(self.parse[0])          # 识别表

        self.parseStrings(self.parse[0])            # 解析字符串

        # 如果启用重命名，对标识符进行重命名处理
        if rename:
            self.renameIdentifiers(self.parse[0])

        self.tokens = SqlangParser.getTokens(self.parse)   # 获取 SQL 查询语句中的所有标记

    @staticmethod
    # 静态方法：从解析的 SQL 结构中提取所有标记并扁平化为列表
    def getTokens(parse):
        flatParse = []
        for expr in parse:
            for token in expr.flatten():
                if token.ttype == STRING:
                    flatParse.extend(str(token).split(' '))
                else:
                    flatParse.append(str(token))
        return flatParse

    # 递归方法：移除输入的 TokenList 中的所有空白符
    def removeWhitespaces(self, tok):
        if isinstance(tok, sqlparse.sql.TokenList):
            tmpChildren = []
            for c in tok.tokens:
                if not c.is_whitespace:
                    tmpChildren.append(c)

            tok.tokens = tmpChildren
            for c in tok.tokens:
                self.removeWhitespaces(c)

    def identifySubQueries(self, tokenList):
        isSubQuery = False

        for tok in tokenList.tokens:     # 如果当前 token 是一个 TokenList，说明可能是一个子查询
            if isinstance(tok, sqlparse.sql.TokenList):    # 递归调用 identifySubQueries 方法来识别子查询
                subQuery = self.identifySubQueries(tok)    # 如果是子查询并且当前 token 是括号（Parenthesis），则标记为 SUBQUERY 类型
                if (subQuery and isinstance(tok, sqlparse.sql.Parenthesis)):
                    tok.ttype = SUBQUERY   # 如果当前 token 是 "select"，则表明这是一个子查询
            elif str(tok) == "select":
                isSubQuery = True
        return isSubQuery

    def identifyLiterals(self, tokenList):
        blankTokens = [sqlparse.tokens.Name, sqlparse.tokens.Name.Placeholder]
        blankTokenTypes = [sqlparse.sql.Identifier]

        for tok in tokenList.tokens:
            # 如果当前 token 是一个 TokenList，将其标记为 INTERNAL 类型
            if isinstance(tok, sqlparse.sql.TokenList):
                tok.ptype = INTERNAL
                self.identifyLiterals(tok)
            # 如果当前 token 是关键字或者是 "select"，则标记为 KEYWORD 类型
            elif tok.ttype == sqlparse.tokens.Keyword or str(tok) == "select":
                tok.ttype = KEYWORD
            # 如果当前 token 是整数或者数值类型的整数，标记为 INTEGER 类型
            elif tok.ttype == sqlparse.tokens.Number.Integer or tok.ttype == sqlparse.tokens.Literal.Number.Integer:
                tok.ttype = INTEGER
            # 如果当前 token 是十六进制数或者数值类型的十六进制数，标记为 HEX 类型
            elif tok.ttype == sqlparse.tokens.Number.Hexadecimal or tok.ttype == sqlparse.tokens.Literal.Number.Hexadecimal:
                tok.ttype = HEX
            # 如果当前 token 是浮点数或者数值类型的浮点数，标记为 FLOAT 类型
            elif tok.ttype == sqlparse.tokens.Number.Float or tok.ttype == sqlparse.tokens.Literal.Number.Float:
                tok.ttype = FLOAT
            # 如果当前 token 是符号字符串、单引号字符串或者数值类型的单引号字符串、符号字符串，标记为 STRING 类型
            elif tok.ttype == sqlparse.tokens.String.Symbol or tok.ttype == sqlparse.tokens.String.Single or tok.ttype == sqlparse.tokens.Literal.String.Single or tok.ttype == sqlparse.tokens.Literal.String.Symbol:
                tok.ttype = STRING
            # 如果当前 token 是通配符，标记为 WILDCARD 类型
            elif tok.ttype == sqlparse.tokens.Wildcard:
                tok.ttype = WILDCARD
            # 如果当前 token 是空白字符类型或者是空白字符类型的标识符，标记为 COLUMN 类型
            elif tok.ttype in blankTokens or isinstance(tok, blankTokenTypes[0]):
                tok.ttype = COLUMN

    def identifyFunctions(self, tokenList):
        for tok in tokenList.tokens:
            # 如果当前 token 是一个函数（Function），则设置 parseTreeSentinel 为 True
            if isinstance(tok, sqlparse.sql.Function):
                self.parseTreeSentinel = True
            # 如果当前 token 是括号（Parenthesis），则设置 parseTreeSentinel 为 False
            elif isinstance(tok, sqlparse.sql.Parenthesis):
                self.parseTreeSentinel = False
            # 如果 parseTreeSentinel 为 True，则将当前 token 标记为 FUNCTION 类型
            if self.parseTreeSentinel:
                tok.ttype = FUNCTION
            # 如果当前 token 是一个 TokenList，递归调用 identifyFunctions 方法
            if isinstance(tok, sqlparse.sql.TokenList):
                self.identifyFunctions(tok)

    def identifyTables(self, tokenList):
        # 如果 tokenList 的 ptype 是 SUBQUERY 类型，则将 False 压入 tableStack 栈中
        if tokenList.ptype == SUBQUERY:
            self.tableStack.append(False)

        for i in range(len(tokenList.tokens)):
            prevtok = tokenList.tokens[i - 1]
            tok = tokenList.tokens[i]

            # 如果当前 token 是 "."，并且之前的 token 类型是 COLUMN，将其类型标记为 TABLE
            if str(tok) == "." and tok.ttype == sqlparse.tokens.Punctuation and prevtok.ttype == COLUMN:
                prevtok.ttype = TABLE

            # 如果当前 token 是 "from"，并且类型是 Keyword，则将 tableStack 栈顶元素设置为 True
            elif str(tok) == "from" and tok.ttype == sqlparse.tokens.Keyword:
                self.tableStack[-1] = True

            # 如果当前 token 是 "where"、"on"、"group"、"order"、"union"，并且类型是 Keyword，则将 tableStack 栈顶元素设置为 False
            elif (str(tok) == "where" or str(tok) == "on" or str(tok) == "group" or str(tok) == "order" or str(
                    tok) == "union") and tok.ttype == sqlparse.tokens.Keyword:
                self.tableStack[-1] = False

            # 如果当前 token 是一个 TokenList，递归调用 identifyTables 方法
            if isinstance(tok, sqlparse.sql.TokenList):
                self.identifyTables(tok)

            # 如果当前 token 类型是 COLUMN，并且 tableStack 栈顶元素为 True，则将其类型标记为 TABLE
            elif tok.ttype == COLUMN:
                if self.tableStack[-1]:
                    tok.ttype = TABLE

        # 如果 tokenList 的 ptype 是 SUBQUERY 类型，则弹出 tableStack 栈顶元素
        if tokenList.ptype == SUBQUERY:
            self.tableStack.pop()

    #用于返回对象的字符串表示形式
    def __str__(self):
        return ' '.join([str(tok) for tok in self.tokens])

    #将对象转换为一个字符串，以便于打印或显示
    def parseSql(self):
        return [str(tok) for tok in self.tokens]
#############################################################################

#############################################################################
#缩略词处理
def revert_abbrev(line):
    # 定义缩写还原的正则表达式模式
    pat_is = re.compile("(it|he|she|that|this|there|here)(\"s)", re.I)  # 缩写 's 形式替换为 is
    pat_s1 = re.compile("(?<=[a-zA-Z])\"s")  # 单词内的 's 替换为空字符串
    pat_s2 = re.compile("(?<=s)\"s?")  # 复数形式中 's 替换为空字符串
    pat_not = re.compile("(?<=[a-zA-Z])n\"t")  # 缩写 n't 替换为 not
    pat_would = re.compile("(?<=[a-zA-Z])\"d")  # 缩写 'd 替换为 would
    pat_will = re.compile("(?<=[a-zA-Z])\"ll")  # 缩写 'll 替换为 will
    pat_am = re.compile("(?<=[I|i])\"m")  # 缩写 'm 替换为 am
    pat_are = re.compile("(?<=[a-zA-Z])\"re")  # 缩写 're 替换为 are
    pat_ve = re.compile("(?<=[a-zA-Z])\"ve")  # 缩写 've 替换为 have

    # 应用正则表达式替换缩写
    line = pat_is.sub(r"\1 is", line)
    line = pat_s1.sub("", line)
    line = pat_s2.sub("", line)
    line = pat_not.sub(" not", line)
    line = pat_would.sub(" would", line)
    line = pat_will.sub(" will", line)
    line = pat_am.sub(" am", line)
    line = pat_are.sub(" are", line)
    line = pat_ve.sub(" have", line)

    return line

def get_wordpos(tag):
    # 根据词性标记返回对应的WordNet词性常量
    if tag.startswith('J'):
        return wordnet.ADJ
    elif tag.startswith('V'):
        return wordnet.VERB
    elif tag.startswith('N'):
        return wordnet.NOUN
    elif tag.startswith('R'):
        return wordnet.ADV
    else:
        return None

def process_nl_line(line):
    # 对自然语言句子进行预处理和格式化
    line = revert_abbrev(line)  # 还原缩写
    line = re.sub('\t+', '\t', line)  # 合并多个制表符为一个
    line = re.sub('\n+', '\n', line)  # 合并多个换行符为一个
    line = line.replace('\n', ' ')  # 将换行符替换为空格
    line = line.replace('\t', ' ')  # 将制表符替换为空格
    line = re.sub(' +', ' ', line)  # 合并多个空格为一个空格
    line = line.strip()  # 去除句子两端的空白字符
    line = inflection.underscore(line)  # 将驼峰命名法转换为下划线分隔

    # 去除括号内的内容
    space = re.compile(r"\([^\(|^\)]+\)")
    line = re.sub(space, '', line)

    line = line.strip()  # 再次去除句子两端的空白字符
    return line

def process_sent_word(line):
    # 对句子进行分词处理
    line = re.findall(r"[\w]+|[^\s\w]", line)  # 分词，保留标点符号
    line = ' '.join(line)  # 将分词结果重新组合成字符串

    # 替换特定格式的词语
    decimal = re.compile(r"\d+(\.\d+)+")  # 小数
    line = re.sub(decimal, 'TAGINT', line)
    string = re.compile(r'\"[^\"]+\"')  # 字符串
    line = re.sub(string, 'TAGSTR', line)
    decimal = re.compile(r"0[xX][A-Fa-f0-9]+")  # 十六进制数
    line = re.sub(decimal, 'TAGINT', line)
    number = re.compile(r"\s?\d+\s?")  # 数字
    line = re.sub(number, ' TAGINT ', line)
    other = re.compile(r"(?<![A-Z|a-z|_|])\d+[A-Za-z]+")  # 混合形式的字符和数字
    line = re.sub(other, 'TAGOER', line)

    cut_words = line.split(' ')  # 将处理后的句子按空格分割成词语列表
    cut_words = [x.lower() for x in cut_words]  # 将所有词语转换为小写

    # 获取词性标记并进行词形还原和词干提取
    word_tags = pos_tag(cut_words)  # 获取词性标记
    tags_dict = dict(word_tags)  # 转换为词语-词性的字典
    word_list = []
    for word in cut_words:
        word_pos = get_wordpos(tags_dict[word])  # 获取词语的WordNet词性
        if word_pos in ['a', 'v', 'n', 'r']:
            word = wnler.lemmatize(word, pos=word_pos)  # 进行词性还原
        word = wordnet.morphy(word) if wordnet.morphy(word) else word  # 进行词干提取
        word_list.append(word)  # 将处理后的词语添加到列表中

    return word_list  # 返回处理后的词语列表

#############################################################################

def filter_all_invachar(line):
    # 去除非常用符号；防止解析有误
    line = re.sub('[^(0-9|a-z|A-Z|\-|_|\'|\"|\-|\(|\)|\n)]+', ' ', line)
    # 包括\r\t也清除了
    # 中横线
    line = re.sub('-+', '-', line)
    # 下划线
    line = re.sub('_+', '_', line)
    # 去除横杠
    line = line.replace('|', ' ').replace('¦', ' ')
    return line


def filter_part_invachar(line):
    #去除非常用符号；防止解析有误
    line= re.sub('[^(0-9|a-z|A-Z|\-|#|/|_|,|\'|=|>|<|\"|\-|\\|\(|\)|\?|\.|\*|\+|\[|\]|\^|\{|\}|\n)]+',' ', line)
    #包括\r\t也清除了
    # 中横线
    line = re.sub('-+', '-', line)
    # 下划线
    line = re.sub('_+', '_', line)
    # 去除横杠
    line = line.replace('|', ' ').replace('¦', ' ')
    return line

########################主函数：代码的tokens#################################
def sqlang_code_parse(line):
    line = filter_part_invachar(line)
    line = re.sub('\.+', '.', line)
    line = re.sub('\t+', '\t', line)
    line = re.sub('\n+', '\n', line)
    line = re.sub(' +', ' ', line)

    line = re.sub('>>+', '', line)#新增加
    line = re.sub(r"\d+(\.\d+)+",'number',line)#新增加 替换小数

    line = line.strip('\n').strip()
    line = re.findall(r"[\w]+|[^\s\w]", line)
    line = ' '.join(line)

    try:
        query = SqlangParser(line, regex=True)
        typedCode = query.parseSql()
        typedCode = typedCode[:-1]
        # 骆驼命名转下划线
        typedCode = inflection.underscore(' '.join(typedCode)).split(' ')

        cut_tokens = [re.sub("\s+", " ", x.strip()) for x in typedCode]
        # 全部小写化
        token_list = [x.lower()  for x in cut_tokens]
        # 列表里包含 '' 和' '
        token_list = [x.strip() for x in token_list if x.strip() != '']
        # 返回列表
        return token_list
    # 存在为空的情况，词向量要进行判断
    except:
        return '-1000'
########################主函数：代码的tokens#################################


#######################主函数：句子的tokens##################################

def sqlang_query_parse(line):
    line = filter_all_invachar(line)
    line = process_nl_line(line)
    word_list = process_sent_word(line)
    # 分完词后,再去掉 括号
    for i in range(0, len(word_list)):
        if re.findall('[\(\)]', word_list[i]):
            word_list[i] = ''
    # 列表里包含 '' 或 ' '
    word_list = [x.strip() for x in word_list if x.strip() != '']
    # 解析可能为空

    return word_list


def sqlang_context_parse(line):
    line = filter_part_invachar(line)
    line = process_nl_line(line)
    word_list = process_sent_word(line)
    # 列表里包含 '' 或 ' '
    word_list = [x.strip() for x in word_list if x.strip() != '']
    # 解析可能为空
    return word_list

#######################主函数：句子的tokens##################################


if __name__ == '__main__':
    print(sqlang_code_parse('""geometry": {"type": "Polygon" , 111.676,"coordinates": [[[6.69245274714546, 51.1326962505233], [6.69242714158622, 51.1326908883821], [6.69242919794447, 51.1326955158344], [6.69244041615532, 51.1326998744549], [6.69244125953742, 51.1327001609189], [6.69245274714546, 51.1326962505233]]]} How to 123 create a (SQL  Server function) to "join" multiple rows from a subquery into a single delimited field?'))
    print(sqlang_query_parse("change row_height and column_width in libreoffice calc use python tagint"))
    print(sqlang_query_parse('MySQL Administrator Backups: "Compatibility Mode", What Exactly is this doing?'))
    print(sqlang_code_parse('>UPDATE Table1 \n SET Table1.col1 = Table2.col1 \n Table1.col2 = Table2.col2 FROM \n Table2 WHERE \n Table1.id =  Table2.id'))
    print(sqlang_code_parse("SELECT\n@supplyFee:= 0\n@demandFee := 0\n@charedFee := 0\n"))
    print(sqlang_code_parse('@prev_sn := SerialNumber,\n@prev_toner := Remain_Toner_Black\n'))
    print(sqlang_code_parse(' ;WITH QtyCTE AS (\n  SELECT  [Category] = c.category_name\n          , [RootID] = c.category_id\n          , [ChildID] = c.category_id\n  FROM    Categories c\n  UNION ALL \n  SELECT  cte.Category\n          , cte.RootID\n          , c.category_id\n  FROM    QtyCTE cte\n          INNER JOIN Categories c ON c.father_id = cte.ChildID\n)\nSELECT  cte.RootID\n        , cte.Category\n        , COUNT(s.sales_id)\nFROM    QtyCTE cte\n        INNER JOIN Sales s ON s.category_id = cte.ChildID\nGROUP BY cte.RootID, cte.Category\nORDER BY cte.RootID\n'))
    print(sqlang_code_parse("DECLARE @Table TABLE (ID INT, Code NVARCHAR(50), RequiredID INT);\n\nINSERT INTO @Table (ID, Code, RequiredID)   VALUES\n    (1, 'Physics', NULL),\n    (2, 'Advanced Physics', 1),\n    (3, 'Nuke', 2),\n    (4, 'Health', NULL);    \n\nDECLARE @DefaultSeed TABLE (ID INT, Code NVARCHAR(50), RequiredID INT);\n\nWITH hierarchy \nAS (\n    --anchor\n    SELECT  t.ID , t.Code , t.RequiredID\n    FROM @Table AS t\n    WHERE t.RequiredID IS NULL\n\n    UNION ALL   \n\n    --recursive\n    SELECT  t.ID \n          , t.Code \n          , h.ID        \n    FROM hierarchy AS h\n        JOIN @Table AS t \n            ON t.RequiredID = h.ID\n    )\n\nINSERT INTO @DefaultSeed (ID, Code, RequiredID)\nSELECT  ID \n        , Code \n        , RequiredID\nFROM hierarchy\nOPTION (MAXRECURSION 10)\n\n\nDECLARE @NewSeed TABLE (ID INT IDENTITY(10, 1), Code NVARCHAR(50), RequiredID INT)\n\nDeclare @MapIds Table (aOldID int,aNewID int)\n\n;MERGE INTO @NewSeed AS TargetTable\nUsing @DefaultSeed as Source on 1=0\nWHEN NOT MATCHED then\n Insert (Code,RequiredID)\n Values\n (Source.Code,Source.RequiredID)\nOUTPUT Source.ID ,inserted.ID into @MapIds;\n\n\nUpdate @NewSeed Set RequiredID=aNewID\nfrom @MapIds\nWhere RequiredID=aOldID\n\n\n/*\n--@NewSeed should read like the following...\n[ID]  [Code]           [RequiredID]\n10....Physics..........NULL\n11....Health...........NULL\n12....AdvancedPhysics..10\n13....Nuke.............12\n*/\n\nSELECT *\nFROM @NewSeed\n"))



