package jdbc

import getl.proc.*
import getl.data.Field
import getl.h2.H2Connection
import getl.jdbc.*
import getl.stat.ProcessTime
import getl.tfs.TDS
import getl.utils.*
import groovy.sql.Sql

import init.*

/**
 * 
 * @author owner
 *
 */
class UnionDataset extends Job {
	def countRow = 1000000
	def countField = 100
	
	TDS h2 = new TDS()
	TableDataset table1 = new TableDataset(connection: h2, tableName: "table1")
	TableDataset table2 = new TableDataset(connection: h2, tableName: "table2")
	
	TDS h2_reader = new TDS()
	TableDataset table1_reader = new TableDataset(connection: h2_reader, tableName: table1.tableName)

	static main(args) {
		new UnionDataset().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}

	@Override
	public void process() {
		// Create table1 (source for merge)
		table1.field << new Field(name: "id", type: "INTEGER", isKey: true)
		table1.field << new Field(name: "name", length: 50, isNull: false)
		(1..countField).each { valField ->
			table1.field << new Field(name: "value$valField", type: "INTEGER", isNull: false)
		}
		table1.create(ifNotExists: true)
		table1.truncate()
		
		// Create table2 (target for merge)
		table2.field << new Field(name: "table_id", type: "INTEGER", isKey: true)
		table2.field << new Field(name: "name", length: 50, isNull: false)
		table2.field << new Field(name: "value", type: "INTEGER", isNull: false)
		table2.create(ifNotExists: true)
		table2.truncate()
		
		def pt = new ProcessTime(name: "Insert records to table1")
		def count = new Flow().writeTo(dest: table1) { updater ->
			(1..countRow).each { num ->
				def row = [:]
				row.id = num
				row.name = "record $num"
				(1..countField).each { valField ->
					row."value$valField" = num
				}
				updater(row)
			}
		}
		pt.finish(count)
		
		// Merge (adding new) table1 to table2
		pt = new ProcessTime(name: "Insert into table2 from table1")
		def valMerge = table1.sqlFields("{orig}", ["table_id", "name"]).join("+")
		count = table2.unionDataset(source: table1, operation: "MERGE", map: [table_id: "id", value: valMerge])
		pt.finish(count)
		
		// Update exists records from table1
		pt = new ProcessTime(name: "Update table1")
		count = new Flow().copy(source: table1_reader, dest: table1, dest_operation: "UPDATE") { in_row, out_row ->
			out_row.name = "${in_row.name} updated"
			
			(1..countField).each { valField ->
				out_row."value$valField" = in_row."value$valField" + 1
			}
		}
		pt.finish(count)
		
		// Merge (update exists) table1 to table2
		pt = new ProcessTime(name: "Update table2 from table1")
		count = table2.unionDataset(source: table1, operation: "MERGE", map: [table_id: "id", value: valMerge])
		pt.finish(count)
	}
}
