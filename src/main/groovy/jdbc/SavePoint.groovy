package jdbc

import getl.proc.*
import getl.utils.*
import getl.csv.*
import getl.data.Field
import getl.h2.H2Connection
import getl.jdbc.*
import init.GenerateData

class SavePoint extends Job {
	H2Connection h2 = new H2Connection(config: "h2")
	TableDataset h2_table = new TableDataset(connection: h2, tableName: "DATA")
	
	CSVConnection csvCon = new CSVConnection(config: "csv")
	CSVDataset csv_file_merge = new CSVDataset(connection: csvCon, fileName: "CopyJDBCToCSVWithPointMerge.txt", header: true, 
											field: [new Field(name: "source_id", type: "INTEGER", isNull: false, isKey: true)])
	CSVDataset csv_file_insert = new CSVDataset(connection: csvCon, fileName: "CopyJDBCToCSVWithPointInsert.txt", header: true,
		field: [new Field(name: "source_id", type: "NUMERIC", isNull: false, isKey: true, length: 38, precision: 0)])
	
	SavePointManager points_merge = new SavePointManager(connection: h2, tableName: "points_merge", saveMethod: "MERGE", 
													fields: [source: "source_id", type: "type_id", time: "fix_dt"])
	
	SavePointManager points_insert = new SavePointManager(connection: h2, tableName: "points_insert", saveMethod: "INSERT",
		fields: [source: "source_id", type: "type_id", time: "fix_dt"])
	
	static main(args) {
		new SavePoint().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}

	@Override
	public void process() {
//		points_merge.drop(true)
		points_merge.create(true)
		
		def last_id = points_merge.lastValue("data.id").value?:0
		Logs.Info("Last point id with points_merge: $last_id")
			
		Logs.Info("Copy rows for ${last_id + 1} to ${last_id + 100}")
		new Flow().copy(source: h2_table, source_where: "source_id > $last_id", source_order: ["source_id"], source_limit: 100, 
						dest: csv_file_merge) { row_in, row_out ->
			if (last_id < row_in.source_id) last_id = row_in.source_id
		}
			
		points_merge.saveValue("data.id", last_id)
		Logs.Info("Set new point id to points_merge: $last_id")
		
		println "TABLE POINTS_MERGE ROWS:"
		points_merge.table.eachRow { println it }
		println ""
		
//		points_insert.drop(true)
		points_insert.create(true)
		
		last_id = points_insert.lastValue("data.id").value?:0
		Logs.Info("Last point id with points_insert: $last_id")
			
		Logs.Info("Copy rows for ${last_id + 1} to ${last_id + 100}")
		new Flow().copy(source: h2_table, source_where: "source_id > $last_id", source_order: ["source_id"], source_limit: 100,
						dest: csv_file_insert) { row_in, row_out ->
			if (last_id < row_out.source_id) last_id = row_out.source_id
		}
			
		points_insert.saveValue("data.id", last_id)
		Logs.Info("Set new point id to points_insert: $last_id")
		
		println "TABLE POINTS_INSERT ROWS:"
		points_insert.table.eachRow { println it }
		println ""
	}
}
