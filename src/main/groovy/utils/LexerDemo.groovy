package utils

import getl.stat.ProcessTime
import getl.utils.*
import getl.exception.ExceptionParser
import getl.proc.Job

class LexerDemo extends Job {
	static def sql = '''
INSERT INTO "schema".table ("id", "name", "time", "value") VALUES (1, "test 1", TO_DATE('9999-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), .1234);
UPDATE schema.table SET name = "test 1-1", time = TO_DATE('9999-01-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), value = .234 WHERE id = 1;
DELETE FROM schema."table" WHERE id = 1;
'''
								
	static main(args) {
		new LexerDemo().run(args)
	}

	@Override
	public void process() {
		lexerDemo(true, sql)
		
		sqlParserDemo(true, sql)
		
		def c = 10000

		def ptLexer = new ProcessTime(name: "parse text")
		(1..c).each { lexerDemo(false, sql) }
		ptLexer.finish(c)

		def ptParser = new ProcessTime(name: "parse SQL statements")
		(1..c).each { sqlParserDemo(false, sql) }
		ptParser.finish(c)

	}
	
	public void lexerDemo(boolean printResult, String content) {
		Lexer lexer = new Lexer(input: new StringReader(content))
		lexer.parse()
		if (printResult) println lexer
	}
	
	public void sqlParserDemo (boolean printResult, String content) {
		Lexer lexer = new Lexer(input: new StringReader(content))
		lexer.parse()
		
		SQLParser parser = new SQLParser(lexer: lexer)
		
		def num = 0
		def statements = lexer.statements()
		
		assert statements.size() > 0
		statements.each { List tokens ->
			num++
			def res
			def type
			try {
				type = parser.statementType(tokens)
				switch (type) {
					case SQLParser.StatementType.INSERT:
						res = parser.parseInsertStatement(tokens)
						break
					case SQLParser.StatementType.UPDATE:
						res = parser.parseUpdateStatement(tokens)
						break
					case SQLParser.StatementType.DELETE:
						res = parser.parseDeleteStatement(tokens)
						break
				}
			}
			catch (ExceptionParser e) {
				println "ERROR TOKEN:"
				println MapUtils.ToJson([tokens: e.tokens])
				throw e
			}
			if (printResult && res != null) {
				println "*** statement $num with $type ***"
				println "table: ${res.schemaName}.${res.tableName}"
				println "values:"
				res."values".each { field, valueExpr ->
					println "	$field: $valueExpr"
				}
				println "where:"
				res."where"?.each { field, valueExpr ->
					println "	$field: $valueExpr"
				}
				println ""
			}
		}
	}
}
