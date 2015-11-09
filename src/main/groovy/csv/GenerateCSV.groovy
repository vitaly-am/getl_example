package csv

import groovy.transform.Synchronized
import getl.csv.*
import getl.data.*
import getl.proc.*
import getl.stat.*
import getl.utils.*

import init.GenerateData

class GenerateCSV extends Job {
	// Count value fields
	def fieldCount = 30
	
	// Count generate rows
	def rowCount = 100000
	
	// Connection to parent CSV directory
	CSVConnection csvCon = new CSVConnection(config: "csv", createPath: true)
	
	// CSV file
	CSVDataset csvFile = new CSVDataset(connection: csvCon, fileName: "data.csv", autoSchema: true)
	
	static main(args) {
		// Run job process
		new GenerateCSV().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		// Start collestion of statistics
		def pt = new ProcessTime(name: "Generate CSV file")
		
		// Declare fields in dataset
		csvFile.field << new Field(name: "id", type: "INTEGER", isNull: false, isKey: true, extended: [increment: true])
		csvFile.field << new Field(name: "name", isNull: false, trim: true)
		csvFile.field << new Field(name: "time", type: "DATETIME", isNull: false)
		csvFile.field << new Field(name: "iseven", type: "BOOLEAN", isNull: false, format: "Y|N")

		// Addiding value fields		
		(1..fieldCount).each { field ->
			csvFile.field << new Field(name: "value_${field}", type: "NUMERIC", length:9, precision: 2)
		}
		
		def saySave = { num ->
			println "Save ${num * 1000} rows"
		}
		
		def curRecord = new SynchronizeObject()
		
		def executor = new Executor(waitTime: 500)
		executor.startBackground {
			println "Current number: ${curRecord.count}"
		}
		
		try {
			// Writing the data flow
			new Flow().writeTo(dest: csvFile, dest_append: false, dest_batchSize: 1000, dest_onSaveBatch: saySave) { Closure updater -> // Closure(Map row)
				// Generates records
				(1..rowCount).each { num ->
					curRecord.nextCount()
					
					// Declare row
					Map row = [:]
					
					// Set value in fields
					row.id = num
					row.name = "Record is ${num}"
					row.time = new Date()
					row.iseven = NumericUtils.IsEven(num)
					(1..fieldCount).each { field ->
						BigDecimal v = new BigDecimal(num)
						row.put("value_" + field, v)
					}  
	
					// Write record to flow				
					updater(row)
				}
			}
	
			// Finish collestion of statistics
			pt.finish(curRecord.count)
		}
		finally {
			executor.stopBackground()
		}
	}

}
