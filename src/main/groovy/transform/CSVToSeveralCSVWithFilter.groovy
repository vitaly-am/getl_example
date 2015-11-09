package transform

import getl.transform.MultipleDataset
import getl.proc.*
import getl.data.*
import getl.csv.*
import getl.utils.*
import getl.stat.*

import init.GenerateData

class CSVToSeveralCSVWithFilter extends Job {
	CSVConnection con = new CSVConnection(config: "csv")
	CSVDataset dss = new CSVDataset(connection: con, fileName: "multiple_ds_source.txt", manualSchema: true)
	CSVDataset ds1 = new CSVDataset(connection: con, fileName: "multiple_ds_all.txt") 
	CSVDataset ds2 = new CSVDataset(connection: con, fileName: "multiple_ds_small.txt")
	CSVDataset ds3 = new CSVDataset(connection: con, fileName: "multiple_ds_big.txt")
	
	MultipleDataset mds = new MultipleDataset(
							dest: ["all": ds1, "<=50000": ds2, ">50000": ds3], 
							condition: ["<=50000": { row -> row.id <= 50000 }, ">50000": { row -> row.id > 50000 }]
						)

	static main(args) {
		new CSVToSeveralCSVWithFilter().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	@Override
	public void process() {
		dss.field << new Field(name: "id", type: "INTEGER")
		dss.field << new Field(name: "name")
		
		ds1.field = dss.field
		
		def pt1 = new ProcessTime(name: "Generate source")
		new Flow().writeTo(dest: dss) { updater ->
			(1..100000).each { num ->
				Map row = [:]
				row.id = num
				row.name = "name ${num}"
				updater(row) 
			}
		}
		
		pt1.finish(dss.writeRows)

		def pt2 = new ProcessTime(name: "Filtered write to datasets")
		def count = new Flow().copy(source: dss, dest: mds)
		pt2.finish(count)
	}
}
