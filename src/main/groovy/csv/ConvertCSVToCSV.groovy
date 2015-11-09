package csv

import getl.csv.*
import getl.data.*
import getl.proc.*
import getl.stat.*
import getl.utils.*

import init.GenerateData

class ConvertCSVToCSV extends Job {
	// Connection for file
	CSVConnection csvCon = new CSVConnection(config: "csv")

	// Source CSV file (for generation run CSVWrite example)
	CSVDataset inFile = new CSVDataset(connection: csvCon, fileName: "data.csv", autoSchema: true)

	// Destination CSV file
	CSVDataset prepareFile = new CSVDataset(connection: csvCon, fileName: "PrepareBulkCSV.txt", rowDelimiter: "\u0001", 
											fieldDelimiter: "\u0002", quoteStr: "\u0003")
	
	// 
	CSVDataset decodePrepareFile = new CSVDataset(connection: csvCon, fileName: "PrepareBulkCSVDecode.txt")

	static main(args) {
		new ConvertCSVToCSV().run()
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	

	void process () {
		// Read counts rows from out file
		def pt1 = new ProcessTime(name: "prepare bulk file")
		def count1 = prepareFile.prepareCSVForBulk(inFile)
		pt1.finish(count1)
		
		// Read counts rows from out file
		def pt2 = new ProcessTime(name: "decode file", objectName: "mb")
		def count2 = decodePrepareFile.decodeBulkCSV(prepareFile)
		pt2.finish((count2 / 1024).longValue())
		
		decodePrepareFile.field = inFile.field
		
		def pt3 = new ProcessTime(name: "Read rows")
		def count3 = decodePrepareFile.readRowCount()
		pt3.finish(count3)
		
		def pt4 = new ProcessTime(name: "Read lines", objectName: "line")
		def count4 = decodePrepareFile.readLinesCount()
		pt4.finish(count4)
	}
}
