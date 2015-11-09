package temporarydata

import getl.csv.*
import getl.data.*
import getl.proc.*
import getl.stat.*
import getl.jdbc.*
import getl.h2.*
import getl.utils.*
import getl.tfs.TDS

import init.GenerateData

class CopyCSVToTDS extends Job {
	// Connection for file
	CSVConnection csvCon = new CSVConnection(config: "csv")
	
	// Source CSV file
	CSVDataset csv_file = new CSVDataset(connection: csvCon, fileName: "data.csv", autoSchema: true)
	
	// Destinition table
	TableDataset table = TDS.dataset(tableName: "DATA")
	
	static main (args) {
		new CopyCSVToTDS().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
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
		table.create()
		
		table.truncate()
		
		def pt = new ProcessTime(name: "Copy CSV file to H2 table with bulk insert")
	
		// Run copy bulk insert proccess
		def num = 0 
		def count = new Flow().copy(source: csv_file, dest: table, clear: true,
									map: ["source_id":"id", "flag":"iseven;format=Y|N", "dt": "time;format=dd.MM.yyyy HH:mm:ss"],
									dest_batchSize: 1000) { source, dest -> // Calculation and transformation code
									
			assert source.id != null
					
			num++				
			dest.seq_id = num
			dest.value_1_2 = source.value_1 + source.value_2
		}
		
		pt.finish(count)
		
		table.truncate()
		
		pt = new ProcessTime(name: "Copy CSV file to H2 table with bulk copy")
		num = 0
		
		// Run copy bulk copy proccess
		table.removeField("id")
		count = new Flow().copy(source: csv_file, dest: table, clear: true,
									map: ["source_id":"id", "flag":"iseven;format=Y|N", "dt": "time;format=dd.MM.yyyy HH:mm:ss"],
									bulkLoad: true, debug: true) { source, dest -> // Calculation and transformation code
			assert source.id != null
									
			num++				
			dest.seq_id = num
			dest.value_1_2 = source.value_1 + source.value_2
		}
		
		pt.finish(count)
	}
}
