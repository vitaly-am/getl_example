package jdbc

import getl.h2.*
import getl.data.*
import getl.jdbc.*
import getl.proc.*
import getl.utils.*
import getl.stat.*

import init.GenerateData

class WriteAllToJDBC extends Job {
	// Count rows for write
	def countRows = 100000
	
	// Count child rows on one parent row
	def countChildRows = 10

	// How method use	
	def testInsertBatch = true
	def testBulkLoad = true
	
	// Connection to H2 database
	H2Connection h2 = new H2Connection(config: "h2")
	
	// Destinition parent table
	TableDataset tableMain = new TableDataset(connection: h2, tableName: "_main", type: JDBCDataset.Type.LOCAL_TEMPORARY)
	
	// Destinition child table
	TableDataset tableChild = new TableDataset(connection: h2, tableName: "_child", type: JDBCDataset.Type.LOCAL_TEMPORARY)
	
	// Query for valid count rows parent table
	QueryDataset countParent = new QueryDataset(connection: h2, query: "SELECT Count(*) AS count FROM _main")
	
	// Query for valid count rows child table
	QueryDataset countChild = new QueryDataset(connection: h2, query: "SELECT Count(*) AS count FROM _child")
	
	static main(args) {
		new WriteAllToJDBC().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}


	@Override
	public void process() {
		def fieldName = new Field(name: "name", type: "STRING", length: 50, isNull: false)
		def fieldValue =  new Field(name: "value", type: "NUMERIC", length: 15, precision: 2, isNull: false)
		
		tableMain.field << new Field(name: "id", type: "INTEGER", isNull: false, isKey: true)
		tableMain.field << fieldName
		tableMain.field << fieldValue
		tableMain.create()
		
		tableChild.field << new Field(name: "num", type: "INTEGER", isNull: false, isKey: true)
		tableChild.field << new Field(name: "parent_id", type: "INTEGER", isNull: false, isKey: true)
		tableChild.field << fieldValue
		tableChild.create()
		
		def count = 0
		def code = { updater ->
			(1..countRows).each { num ->
				Map row = [:]
				row.id = num
				row.name = GenerationUtils.GenerateValue(fieldName)
				row.value = GenerationUtils.GenerateValue(fieldValue)
				updater("parent", row)
				count++
				
				(1..countChildRows).each { childNum ->
					Map childRow = [:]
					childRow.num = childNum
					childRow.parent_id = num
					childRow.value = GenerationUtils.GenerateValue(fieldValue)
					updater("child", childRow)
					count++
				}
			}
		}
		
		if (testInsertBatch) {
			tableChild.truncate()
			tableMain.truncate()
			
			def pt = new ProcessTime(name: "Write with batch insert")
			count = 0 
			new Flow().writeAllTo(dest: [parent: tableMain, child: tableChild], dest_parent_batchSize: 1000, dest_child_batchSize: 1000 * countChildRows, code)
			pt.finish(count)
			
			def rows = countParent.rows()
			Logs.Info("Count rows in main: ${rows[0].count}")
			rows = countChild.rows()
			Logs.Info("Count rows in child: ${rows[0].count}")
		}
		
		if (testBulkLoad) {
			tableChild.truncate()
			tableMain.truncate()
			
			def pt = new ProcessTime(name: "Write with bulk load")
			count = 0
			new Flow().writeAllTo(dest: [parent: tableMain, child: tableChild], bulkLoad: ["parent", "child"], code)
			pt.finish(count)
			
			def rows = countParent.rows()
			Logs.Info("Count rows in main: ${rows[0].count}")
			rows = countChild.rows()
			Logs.Info("Count rows in child: ${rows[0].count}")
		}
	}
}
