package csv

import getl.csv.*
import getl.data.*
import getl.proc.*
import getl.stat.*
import getl.utils.*

import init.GenerateData

class ProcessCSV extends Job {
	CSVConnection csvCon = new CSVConnection(config: "csv")
	CSVDataset csvFile = new CSVDataset(connection: csvCon, fileName: "data.csv", autoSchema: true)

	static main(args) {
		new ProcessCSV().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		def pt = new ProcessTime(name: "Calc count rows")
		def countRows = csvFile.readRowCount()
		println "$countRows rows from file $csvFile"
		pt.finish(countRows)
		
		if (Config.content."vars"?."init" != null) return
		
		pt = new ProcessTime(name: "Read file")

		int max_id = 0
		int count_even = 0
		Date max_time
		BigDecimal sumValue_1_2 = 0
		def flow = new Flow()
		long i = flow.process(
			source: csvFile, // Source file 
			saveErrors: true,  // Save assertion errors to error dataset in flow
			source_isValid: true, // Validation values of field constraint 
			source_saveErrors: true // Save parse errors to error dataset in source dataset
		) { row ->
			// Validation value rows
			assert !(row.id in 1..10), "id must be great 10"
			assert row.name.length() > 0
			assert row.time.year + 1900 > 2000

			// Calculation
			if (max_id < row.id) max_id = row.id
			if (max_time < row.time) max_time = row.time
			if (row.iseven) count_even++
			sumValue_1_2 += row.value_1 + row.value_2
		}
		pt.finish(i)
		
		Logs.Info("Complete read ${i} rows, max id: ${max_id}, even id:${count_even}, max time: ${DateUtils.FormatDateTime(max_time)}, sum value1+value2: ${sumValue_1_2}")
		
		// Process parse errors
		if (csvFile.isReadError) {
			println ""
			println "Found ${csvFile.errorsDataset.writeRows} errors:"
			new Flow().process(source: csvFile.errorsDataset) { row ->
				println "row ${row.row}: ${row.error}"
			}
		}
		
		// Process assertion errors
		if (flow.errorsDataset.writeRows > 0) {
			println ""
			println "Found ${flow.errorsDataset.writeRows} assert errors:"
			new Flow().process(source: flow.errorsDataset) { row ->
				println "id ${row.id}: ${row.error}"
			}
		}
	}

}
