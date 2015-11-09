package transform

import getl.csv.*
import getl.data.*
import getl.proc.*
import getl.utils.*
import getl.stat.*

import init.GenerateData

class Lookup extends Job {
	CSVConnection csvCon = new CSVConnection(config: "csv")
	CSVDataset inFile = new CSVDataset(connection: csvCon, fileName: "data.csv", autoSchema: true)
	CSVDataset keyFile = new CSVDataset(connection: csvCon, fileName: "lookup.csv", autoSchema: true)
	CSVDataset outFile = new CSVDataset(connection: csvCon, fileName: "example_lookup.txt")
	
	static main(args) {
		new Lookup().run()
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}

	public void process() {
		ProcessTime pt1 = new ProcessTime(name: "Generate lookup keys from CSV data file")
		// Declare fields for lookup file
		keyFile.field << new Field(name: "id", type: "INTEGER", isKey: true)
		keyFile.field << new Field(name: "name", isNull: false)
		pt1.finish(new Flow().copy(source: inFile, dest: keyFile))
		
		// Declare fields for output file with lookup name from CSV data file
		outFile.field = inFile.field
		outFile.field << new Field(name: "lookup_name", type: "STRING")
		
		ProcessTime pt2 = new ProcessTime(name: "Prepare lookup rows")
		// Build lookup list
		def lookup = keyFile.lookup(key: "id", strategy: "HASH")
		pt2.finish(lookup.size())
		
		ProcessTime pt3 = new ProcessTime(name: "Copy datasets with lookup")
		// Copy CSV file to output file with find lookup name
		def count = new Flow().copy(source: inFile, dest: outFile) { ri, ro ->
			ro.lookup_name = lookup.get(ri.id).name
		} 
		pt3.finish(count)
		
	}
}
