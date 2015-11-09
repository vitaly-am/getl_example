package jdbc

import getl.h2.*
import getl.data.*
import getl.jdbc.*
import getl.proc.*
import getl.utils.*
import getl.stat.*

import init.GenerateData

class WriteToJDBC extends Job {
	// Count rows for write
	def countRows = 1000000
	
	// How method use
	def testInsertBatch = true
	def testBulkLoad = true
	
	// Connection to H2 database
	H2Connection h2 = new H2Connection(config: "h2")
	
	// Destinition table
	TableDataset table = new TableDataset(connection: h2, tableName: "_bulkLoad", type: JDBCDataset.Type.LOCAL_TEMPORARY)
	
	// Query for valid count rows
	QueryDataset query = new QueryDataset(connection: h2, query: "SELECT Count(*) AS count FROM _bulkLoad")
	
	static main(args) {
		new WriteToJDBC().run(args)
	}
	
	// Init config parameters for job 
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}


	
	// Job process code
	public void process() {
		def fieldName = new Field(name: "name", type: "STRING", length: 50, isNull: false)
		def fieldValue =  new Field(name: "value", type: "NUMERIC", length: 15, precision: 2, isNull: false)
		
		// Added field to table
		table.field << new Field(name: "id", type: "INTEGER", isNull: false, isKey: true)
		table.field << fieldName
		table.field << fieldValue
		table.create()
		
		// Generation and write row code
		def code = { updater ->
			(1..countRows).each { num ->
				Map row = [:]
				row.id = num
				row.name = GenerationUtils.GenerateValue(fieldName)
				row.value = GenerationUtils.GenerateValue(fieldValue)
				updater(row)
			}
		}
		
		if (testInsertBatch) {
			table.truncate()
			def pt = new ProcessTime(name: "Write with batch insert")
			def count = new Flow().writeTo(dest: table, dest_batchSize: 1000, code)
			pt.finish(count)
		
			def rows = query.rows()
			Logs.Info("Count rows in table: ${rows[0].count}")
		}
		
		
		if (testBulkLoad) {
			table.truncate()
			def pt = new ProcessTime(name: "Write with bulk load")
			def count = new Flow().writeTo(dest: table, bulkLoad: true, code)
			pt.finish(count)
	
			def rows = query.rows()
			Logs.Info("Count rows in table: ${rows[0].count}")
		}
	}
}
