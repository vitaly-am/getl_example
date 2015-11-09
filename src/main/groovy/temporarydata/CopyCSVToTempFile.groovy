package temporarydata

import getl.csv.*
import getl.data.*
import getl.proc.*
import getl.tfs.TFS
import getl.utils.*
import getl.stat.*

import init.GenerateData

class CopyCSVToTempFile extends Job {
	// Connection for file
	CSVConnection csvCon = new CSVConnection(config: "csv")

	// Source CSV file
	CSVDataset csvFile = new CSVDataset(connection: csvCon, fileName: "data.csv", autoSchema: true)
	
	static void main(args) {
		new CopyCSVToTempFile().run()
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		// Read csv file structure
		csvFile.loadDatasetMetadata()
		
		// Get fields without value_* fields
		def f = ListUtils.CopyWhere(csvFile.field) { 
			!it.name.matches("value_.*")
		}
		
		new Flow().with {
			// Write rows to temp1 temporary file
			def pt1 = new ProcessTime(name: "Write to temp")
			def count = writeTo([tempDest: "temp1", tempFields: f]) { updater ->
				(1..1000000).each { num ->
					def row = [:]
					row.id = num
					row.name = "Record num ${num}"
					row.time = new Date()
					row.iseven = false
					updater(row)
				}
			}
			pt1.finish(count)
		
			// Process rows to temp1
			def pt2 = new ProcessTime(name: "Read temp")
			count = process(tempSource: "temp1") { row ->
				assert row.id  > 0
			}
			pt2.finish(count)
			
			// Create temp2 temporary file
			def temp2 = TFS.dataset()
			temp2.isGzFile = true
			temp2.field = f
			
			// Copy rows from CSV to temp2 temporary file
			def pt3 = new ProcessTime(name: "Copy CSV to temp")
			count = copy(source: csvFile, dest: temp2)
			pt3.finish(count)
			
			// Copy rows from temp2 to temp2 temporary file
			def pt4 = new ProcessTime(name: "Copy temp to temp")
			count = copy(source: temp2, tempDest: "temp3")
			pt4.finish(count)
			
			// Process temp3 temporary file
			def pt5 = new ProcessTime(name: "Read temp")
			count = process(tempSource: "temp3") { row ->
				assert row.id > 0
			}
			pt5.finish(count)
		}
	}

}
