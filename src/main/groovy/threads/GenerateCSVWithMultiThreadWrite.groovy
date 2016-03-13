package threads

import getl.csv.*
import getl.data.*
import getl.proc.*
import getl.stat.*
import getl.utils.*

import init.GenerateData

class GenerateCSVWithMultiThreadWrite extends Job {
	CSVConnection csvCon = new CSVConnection(config: "csv")
	CSVDataset csvFile = new CSVDataset(connection: csvCon, fileName: "GenerateCSVWithMultiThreadWrite.csv")

	static main(args) {
		new GenerateCSVWithMultiThreadWrite().run()
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		csvFile.field << new Field(name: "id", type: "INTEGER", isKey: true)
		csvFile.field << new Field(name: "name", type: "STRING", isNull: false)
		csvFile.field << new Field(name: "dt", type: "DATETIME", isNull: false)
		
		def pt = new ProcessTime(name: "Unload to CSV file with 10 thread")
		
		new Flow().writeTo(dest: csvFile, dest_batchSize: 1000, writeSynch: true) { updater ->
			new Executor(mainCode: { Logs.Info("writeln ${csvFile.writeRows} rows")}).runMany(10) { num ->
				def start = (num - 1) * 1000000 + 1
				def finish = num * 1000000
				(start..finish).each { id ->
					Map row = [:]
					row.id = id
					row.name = "rec no ${id} by ${new Date()}"
					row.dt = new Date()
					updater(row)
				}
			}
		}
		
		pt.finish(csvFile.updateRows)
	}
}
