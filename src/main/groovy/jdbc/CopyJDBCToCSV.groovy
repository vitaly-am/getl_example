package jdbc

import getl.csv.*
import getl.data.*
import getl.jdbc.*
import getl.h2.*
import getl.proc.*
import getl.stat.ProcessTime
import getl.utils.*

import init.GenerateData

class CopyJDBCToCSV extends Job {
	// Connection to H2 database  
	H2Connection h2 = new H2Connection(config: "h2")
	
	// Source table (for generation rows run example FlowCSVToTable)
	TableDataset h2_table = new TableDataset(connection: h2, tableName: "DATA")
	
	// Connection for file
	CSVConnection csvCon = new CSVConnection(config: "csv")
	
	// Destinition CSV file
	CSVDataset csv_file = new CSVDataset(connection: csvCon, fileName: "CopyJDBCToCSV.txt", header: true, autoSchema: true)
	
	static main (args) {
		new CopyJDBCToCSV().run()
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		def pt = new ProcessTime(name: "Unload table DATA to CSV")
		
		def fieldPrepare = {
			csv_file.field = h2_table.field
			def i = csv_file.indexOfField("DT")
			csv_file.field.add(i, new Field(name: "DATE", type: Field.Type.DATE))
			csv_file.fieldByName("DT").name = "TIMESTAMP"
		}
		
		def name = "Record is %"
		long count = new Flow().copy(source: h2_table, dest: csv_file, 
							source_where: "NAME LIKE '${name}'",
							source_order: ["ID"],
							map: ["date": "dt", "timestamp": "dt"],
							source_prepare: fieldPrepare,
							dest_batchSize: 1000,
							dest_splitSize: 10000000
						)
		
		pt.finish(count)
		Logs.Info("Complete recording to file ${csv_file.fullFileName()} write ${csv_file.countWriteCharacters()} characters")
	}
}
