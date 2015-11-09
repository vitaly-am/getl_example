package csv

import getl.csv.*
import getl.data.*
import getl.proc.*
import getl.stat.*
import getl.utils.*

import init.GenerateData

class CopyCSVToCSV extends Job {
	// Connection for file
	CSVConnection csvCon = new CSVConnection(config: "csv")

	// Source CSV file (for generation run CSVWrite example)
	CSVDataset inFile = new CSVDataset(connection: csvCon, fileName: "data.csv", autoSchema: true)

	// Destination CSV file
	CSVDataset outFile = new CSVDataset(connection: csvCon, fileName: "CopyCSVToCSV.txt", 
										rowDelimiter: "\r\n", fieldDelimiter: ";", header: true, autoSchema: true, escaped: true)

	static main(args) {
		new CopyCSVToCSV().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	

	void process () {
		// Prepare on read metadata source file
		def in_prepare = { 
			// Copy metadata of source file to metadata of destination file
			outFile.field = inFile.field

			// Change field properties
			outFile.fieldByName("id").name = "source_id"
			outFile.fieldByName("time").format = "yyyy-MM-dd HH:mm:ss"
			outFile.fieldByName("iseven").with {
				name = "flag"
				type = "INTEGER"
			}

			// Remove fields from destinataion file for value_10 to value_50
			outFile.removeFields { Field f ->
				if (f.name.toLowerCase().matches("value_.*")) {
					def i = Integer.valueOf(f.name.substring(6))
					if (i > 9) return true
				}
				false
			}

			// Addiding new field to destination file
			outFile.field << new Field(name: "value_1_2", type: Field.Type.NUMERIC, length: 9, precision: 2)
		}
		
		def pt = new ProcessTime(name: "copy in to out file")

		// Run copy proccess
		def count = new Flow().copy(source: inFile, dest: outFile, source_prepare: in_prepare,  
									excludeFields: ["value_3"], 
									map: ["source_id":"id", "flag":"iseven;format=Y|N", "time": "time;format=dd.MM.yyyy HH:mm:ss"],
									debug: true
									) { source, dest ->
			dest.value_1_2 = source.value_1 + source.value_2
			dest.name = "\"${source.name}\" records in \"out\""
		}

		pt.finish(count)
		
		Logs.Info("Complete recording to file ${outFile.fullFileName()} write ${outFile.countWriteCharacters()} characters")
		
		// Read counts rows from out file
		pt = new ProcessTime(name: "read count rows out file")
		count = outFile.readRowCount()
		pt.finish(count)
		
		// Processing out file
		count = 0
		pt = new ProcessTime(name: "read out file")
		outFile.eachRow { row -> count++ }
		pt.finish(count)
	}
}
