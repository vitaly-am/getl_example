package jdbc

import getl.exception.ExceptionSQLScripter
import getl.h2.H2Connection
import getl.jdbc.SQLScripter
import getl.jdbc.TableDataset
import getl.proc.Job
import getl.utils.*

import init.GenerateData

class RunSQLBatchScript extends Job {
	H2Connection h2 = new H2Connection(config: "h2")
	SQLScripter scripter = new SQLScripter(connection: h2, logEcho: "INFO")
	TableDataset table = new TableDataset(connection: h2, tableName: "TEST_SQL")
	
	static void main (args) {
		new RunSQLBatchScript().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	
	public void process() {
		scripter.script = """
ECHO variable: parent = "{parent}" 

CREATE LOCAL TEMPORARY TABLE TEST_SQL (
  ID int NOT NULL,
  NAME varchar(20)
);

INSERT INTO TEST_SQL VALUES (1, 'test 1');
INSERT INTO TEST_SQL VALUES (2, 'test 2');
INSERT INTO TEST_SQL VALUES (3, 'test 3');
COMMIT;

FOR SELECT ID FROM test_sql;
  FOR SELECT ID AS CHILD_ID FROM TEST_SQL WHERE ID = {ID} + 1; 
    ECHO for: id = "{ID}", child_id = "{CHILD_ID}"
  END FOR; 
END FOR;

IF EXISTS(SELECT * FROM TEST_SQL WHERE ID = 2);
  IF EXISTS(SELECT * FROM TEST_SQL WHERE ID = 3);
    ECHO if: true
  END IF;
END IF;
SET SELECT * FROM TEST_SQL WHERE id = 3;
ECHO set: id = "{id}", name = "{name}"

/*:updated_name*/
UPDATE TEST_SQL
SET NAME = Upper(NAME);
ECHO update: "{updated_name}" rows

/*:select*/
SELECT * FROM TEST_SQL;

ECHO select: {select}

IF (1 = 1);
ERROR Generated error
END IF;
ECHO Test error
"""
//		h2.sqlHistoryFile = "c:/tmp/h2.sql"

		scripter.vars.parent = "Test value"
		try {		
			scripter.runSql()
		}
		catch (ExceptionSQLScripter e) {
			println e.message
		}
		println "All row updated: ${scripter.rowCount}, updated row for name to upper: ${scripter.vars.updated_name}"
		println "Rows returned from script:"
		scripter.vars.select.each { row -> println row }
		
		println "Table rows:"
		table.eachRow { row -> println row }
	}

}
