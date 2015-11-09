package jdbc

import getl.data.*
import getl.jdbc.*
import getl.h2.*
import getl.proc.*
import getl.stat.ProcessTime
import getl.utils.*

import init.GenerateData

class CreateJDBCObjects extends Job {
	// H2 connection to db
	H2Connection db = new H2Connection(config: "h2")
	
	// Table for saving CSV data 
	TableDataset table = new TableDataset(connection: db, tableName: "DATA")
	
	// Temporary table for saving CSV data
	TableDataset tableTemp = new TableDataset(connection: db, tableName: "T_DATA", type: JDBCDataset.Type.GLOBAL_TEMPORARY)

	
	// View based on table
	ViewDataset view = new ViewDataset(connection: db, tableName: "V_DATA")
	
	// Parent-Child tables
	TableDataset tableMain = new TableDataset(connection: db, tableName: "MAIN")
	TableDataset tableChild = new TableDataset(connection: db, tableName: "CHILD")
	
	// SQL scripter
	SQLScripter sql = new SQLScripter(connection: db)
	
	static main (args) {
		new CreateJDBCObjects().run(args)
	}

	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		// Start collestion of statistics
		def pt = new ProcessTime(name: "Generate H2 objects")
		
		// Recreate sequence
		sql.script = """
			DROP SEQUENCE IF EXISTS S_DATA;
			CREATE SEQUENCE IF NOT EXISTS S_DATA INCREMENT BY 100 CACHE 1;
		"""
		sql.runSql()
		
		// Drop view and tables
		view.drop(ifExists: true)
		table.drop(ifExists: true)
		tableMain.drop(ifExists: true)
		tableChild.drop(ifExists: true)
		tableTemp.drop(ifExists: true)
		
		// Declare table DATA structure
		table.field << new Field(name: "ID", type: "BIGINT", isNull: false, isAutoincrement: true, isKey: true)
		table.field << new Field(name: "SEQ_ID", type: "BIGINT", isNull: false)
		table.field << new Field(name: "SOURCE_ID", type: "INTEGER", isNull: false)
		table.field << new Field(name: "NAME", length:50, isNull: false)
		table.field << new Field(name: "DT", type: "DATETIME", isNull: false)
		table.field << new Field(name: "FLAG", type: "BOOLEAN", isNull: false)
		table.field << new Field(name: "VALUE_1", type: "NUMERIC", length: 9, precision: 2)
		table.field << new Field(name: "VALUE_2", type: "NUMERIC", length: 9, precision: 2)
		table.field << new Field(name: "VALUE_3", type: "NUMERIC", length: 9, precision: 2)
		table.field << new Field(name: "VALUE_4", type: "NUMERIC", length: 9, precision: 2)
		table.field << new Field(name: "VALUE_5", type: "NUMERIC", length: 9, precision: 2)
		table.field << new Field(name: "VALUE_6", type: "NUMERIC", length: 9, precision: 2)
		table.field << new Field(name: "VALUE_7", type: "NUMERIC", length: 9, precision: 2)
		table.field << new Field(name: "VALUE_8", type: "NUMERIC", length: 9, precision: 2)
		table.field << new Field(name: "VALUE_9", type: "NUMERIC", length: 9, precision: 2)
		table.field << new Field(name: "VALUE_1_2", type: "NUMERIC", length: 9, precision: 2)
		
		// Declare index DATA_UNIQUE on table DATA
		def idx = [DATA_UNIQUE: [unique: true, columns: ["SEQ_ID"]]]
		
		// Create table DATA
		table.create(indexes: idx)
		
		// Create view V_DATA base on table DATA
		sql.script = "CREATE VIEW IF NOT EXISTS V_DATA AS SELECT * FROM DATA"
		sql.runSql()
		
		// Declare temp table T_DATA structure
		tableTemp.field = table.field
		
		// Create temp table T_DATA
		tableTemp.create(transactional: true)
		
		// Declare table MAIN structure
		tableMain.field << new Field(name: "ID", type: "BIGINT", isNull: false, isKey: true)
		tableMain.field << new Field(name: "NAME", length: 50, isNull: false)
		
		// Create table MAIN
		tableMain.create()
		
		// Declare table CHILD structure
		tableChild.field << new Field(name: "ID", type: "BIGINT", isNull: false, isAutoincrement: true, isKey: true)
		tableChild.field << new Field(name: "MAIN_ID", type: "BIGINT", isNull: false)
		tableChild.field << new Field(name: "NUM", type: "INTEGER", isNull: false)
		
		// Declare index CHILD_UNIQUE on table CHILD 
		def idxChild = [CHILD_UNIQUE: [columns: ["MAIN_ID", "NUM"]]]
		
		// Create table CHILD
		tableChild.create(indexes: idxChild)
		
		pt.finish()
	}
}
