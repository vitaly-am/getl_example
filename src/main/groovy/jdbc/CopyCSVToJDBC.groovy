package jdbc

import getl.csv.*
import getl.data.*
import getl.proc.*
import getl.stat.*
import getl.jdbc.*
import getl.h2.*
import getl.utils.*
import getl.tfs.*

import init.GenerateData

class CopyCSVToJDBC extends Job {
	// Connection for file
	CSVConnection csvCon = new CSVConnection(config: "csv")
	
	// Source CSV file
	CSVDataset csv_file = new CSVDataset(connection: csvCon, fileName: "data.csv", autoSchema: true)
	
	// Connection to H2 database
	H2Connection h2 = new H2Connection(config: "h2")
	
	// Destinition table
	TableDataset table = new TableDataset(connection: h2, tableName: "DATA")
	
	// Sequence
	Sequence seq = new Sequence(connection: h2, name: "S_DATA", cache: 100)
	
	static main (args) {
		new CopyCSVToJDBC().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		table.truncate()
		
		def pt = new ProcessTime(name: "Copy CSV file to H2 table with bulk insert")
	
		// Run copy bulk insert proccess
		def count = new Flow().copy(source: csv_file, dest: table, clear: true,
									map: ["source_id":"id", "flag":"iseven;format=Y|N", "dt": "time;format=dd.MM.yyyy HH:mm:ss"],
									dest_batchSize: 10000) { source, dest -> // Calculation and transformation code
									
			assert source.id != null
									
			dest.seq_id = seq.nextValue
			dest.value_1_2 = source.value_1 + source.value_2
		}
		
		pt.finish(count)
		
		if (Config.content."vars"?."init" != null) return
		
		table.truncate()
		
		pt = new ProcessTime(name: "Copy CSV file to H2 table with bulk copy")
		
		// Run copy bulk copy proccess
		table.removeField("id")
		count = new Flow().copy(source: csv_file, dest: table, clear: true,
									map: ["source_id":"id", "flag":"iseven;format=Y|N", "dt": "time;format=dd.MM.yyyy HH:mm:ss"],
									excludeFields: ["id"],
									bulkLoad: true, debug: true) { source, dest -> // Calculation and transformation code
			assert source.id != null
									
			dest.seq_id = seq.nextValue
			dest.value_1_2 = source.value_1 + source.value_2
		}
	
		pt.finish(count)
	}
}
