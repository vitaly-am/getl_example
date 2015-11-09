package csv

import getl.csv.*
import getl.data.*
import getl.proc.*
import getl.stat.*
import getl.utils.*
import init.GenerateData

class CopyCSVWithSplitToCSV extends Job {
	// Connection for file
	CSVConnection csvCon = new CSVConnection(config: "csv", extension: "csv", isGzFile: true, autoSchema: true)

	// Source CSV file
	CSVDataset sourceFile = new CSVDataset(connection: csvCon, fileName: "CopyCSVToCSVWithSplit")
	
	// Destination CSV file
	CSVDataset destFile = new CSVDataset(connection: csvCon, fileName: "CopyCSVWithSplitToCSV")

	static main(args) {
		new CopyCSVWithSplitToCSV().run()
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	

	void process () {
		def pt = new ProcessTime(log: "INFO")

		// Run copy proccess
		def count = new Flow().copy(source: sourceFile, source_isSplit: true, dest: destFile, inheritFields: true)

		pt.finish(count)
		
		Logs.Info("Complete recording to file ${destFile.fullFileName()}, read ${sourceFile.countReadPortions()} files")
	}
}
