package csv

import getl.csv.*
import getl.data.*
import getl.proc.*
import getl.stat.*
import getl.utils.*

import init.GenerateData

class CopyCSVToCSVWithSplit extends Job {
	// Connection for file
	CSVConnection csvCon = new CSVConnection(config: "csv")

	// Source CSV file (for generation run CSVWrite example)
	CSVDataset sourceFile = new CSVDataset(connection: csvCon, fileName: "data.csv", autoSchema: true)

	// Destination CSV file
	CSVDataset destFile = new CSVDataset(connection: csvCon, fileName: "CopyCSVToCSVWithSplit", extension: "csv", autoSchema: true, isGzFile: true)

	static main(args) {
		new CopyCSVToCSVWithSplit().run()
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	

	void process () {
		def pt = new ProcessTime(log: "INFO")

		// Run copy proccess
		def count = new Flow().copy(source: sourceFile, dest: destFile, dest_splitSize: 10000000, inheritFields: true)

		pt.finish(count)
		
		Logs.Info("Complete recording to file ${destFile.fullFileName()} write ${destFile.countWriteCharacters()} characters with ${destFile.countWritePortions()} files")
	}
}
