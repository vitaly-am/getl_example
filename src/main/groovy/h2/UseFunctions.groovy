package h2

import getl.jdbc.*
import getl.csv.CSVConnection;
import getl.h2.*
import getl.proc.*
import getl.stat.ProcessTime
import getl.tfs.TDS
import getl.utils.*
import init.GenerateData

class UseFunctions extends Job {
	// Connection to H2 database
	H2Connection h2 = new H2Connection(config: "h2")
	
	// Connection for file
	CSVConnection csvCon = new CSVConnection(config: "csv")
	
	// Destinition table (for generation run TableList example)
	TableDataset h2_table = new TableDataset(connection: h2, tableName: "DATA")
	
	static main(args) {
		new UseFunctions().run()
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		h2_table.retrieveFields()
		TDS tempCon = new TDS()
		
		H2Functions.RegisterFunctions(h2)
		H2Functions.RegisterFunctions(tempCon)
		try {
			// Test date function
			def sql_date = "SELECT GETL_FormatDate('yyyy_MM_dd HH_mm_ss', CURRENT_TIMESTAMP) AS date"
			def rows_date = new QueryDataset(connection: h2, query: sql_date).rows()
			println rows_date[0]."date"
			
			// Test select function
			def selectResult = H2Functions.SelectFunction(h2, "GETL_FormatDate", [format: 'yyyy_MM_dd HH_mm_ss', date: DateUtils.Now()])
			println selectResult
			
			def sql_groovy = """
SELECT GETL_RunGroovyScript('
getl.jdbc.QueryDataset qCount = new getl.jdbc.QueryDataset(connection: connection, query: "SELECT Count(*) AS count_rows FROM DATA")
def rows = qCount.rows()

return rows[0]."count_rows"
') AS groovy_result;
"""
			def rows_groovy = new QueryDataset(connection: h2, query: sql_groovy).rows()
			println rows_groovy[0]."groovy_result"
			
			TableDataset tempTable = new TableDataset(connection: tempCon, tableName: "DATA", field: h2_table.field)
			tempTable.drop(ifExists: true)
			tempTable.create()
			//def sql_jdbc_1 = "SELECT GETL_CopyToJDBC('SELECT * FROM DATA', 'getl.h2.H2Connection', '${tempCon.currentConnectURL()}', 'sa', NULL, NULL, NULL, 'DATA', 10000) AS count_rows_jdbc"
			//def rows_jdbc_1 = new QueryDataset(connection: h2, query: sql_jdbc_1).rows()
			def rows_jdbc1 = H2Functions.SelectFunction(h2, "GETL_CopyToJDBC", [
								query: "SELECT * FROM DATA", 
								driver: "getl.h2.H2Connection", 
								url: "${tempCon.currentConnectURL()}", 
								login: "sa", pass: null, db: null, schema: null, table: "DATA", 
								batchSize: 10000 
								])
			println rows_jdbc1
			
			tempTable.truncate()
			def sql_jdbc_2 = "SELECT GETL_CopyFromJDBC('SELECT * FROM DATA', 'getl.h2.H2Connection', '${h2.currentConnectURL()}', 'sa', NULL, NULL, 'DATA', 10000) AS count_rows_jdbc"
			def rows_jdbc_2 = new QueryDataset(connection: tempCon, query: sql_jdbc_2).rows()
			println rows_jdbc_2[0]."count_rows_jdbc"
			
			// Test copy to CSV
			def sql_csv = "SELECT GETL_CopyToCSV('SELECT * FROM DATA', '${csvCon.path}/unload_from_h2.csv', '\t', true) AS count_rows_csv"
			def rows_csv = new QueryDataset(connection: tempCon, query: sql_csv).rows()
			println rows_csv[0]."count_rows_csv"
		}
		finally {
			H2Functions.UnregisterFunctions(h2)
			H2Functions.UnregisterFunctions(tempCon)
		}
	}

}
