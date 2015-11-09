package transform

import getl.transform.*
import getl.proc.*
import getl.stat.ProcessTime
import getl.data.*
import getl.csv.*
import getl.utils.*

import init.GenerateData

class CopyCSVToCSVWithTransformation extends Job {
	CSVConnection con = new CSVConnection(config: "csv")
	CSVDataset ds_source = new CSVDataset(connection: con, fileName: "transformation_source.txt", manualSchema: true)
	CSVDataset ds_all = new CSVDataset(connection: con, fileName: "transformation_all.txt")
	CSVDataset ds_big = new CSVDataset(connection: con, fileName: "transformation_big.txt")
	CSVDataset ds_small = new CSVDataset(connection: con, fileName: "transformation_small.txt")
	
	MultipleDataset mds = new MultipleDataset(
								dest: [all: ds_all, big: ds_big, small: ds_small], 
								condition: [big: { it.count >= 5000}, small: { it.count < 5000 }])
	
	SorterDataset sds = new SorterDataset(dest: mds, fieldOrderBy: ["count", "group"])
	
	AggregatorDataset ads = new AggregatorDataset(
		dest: sds, 
		algorithm: "HASH",
		fieldByGroup: ["group"], 
		fieldCalc: [
			count: [method: "COUNT"], 
			count_even: [method: "COUNT", filter: { row -> new BigDecimal("${row.value}").remainder(2) == 0 }],
			name_min: [fieldName: "name", method: "MIN"],
			name_max: [fieldName: "name", method: "MAX"],
			value_sum: [fieldName: "value"],
			value_min: [fieldName: "value", method: "MIN"],
			value_max: [fieldName: "value", method: "MAX"]
		]
	)
	
	static main(args) {
		new CopyCSVToCSVWithTransformation().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	public void declareSourceFields() {
		ds_source.field.clear()
		ds_source.field << new Field(name: "group", type: "INTEGER")
		ds_source.field << new Field(name: "name")
		ds_source.field << new Field(name: "value", type: "INTEGER")
	}
	
	public void generateSource() {
		declareSourceFields()
		
		def pt = new ProcessTime(name: "Generate transformation source")
		
		def rnd = new Random()
		new Flow().writeTo(dest: ds_source) { updater ->
			(0..100).each { group ->
				def maxnum = rnd.nextInt(9999)
				(1..maxnum).each { num ->
					Map row = [:]
					if (group > 0) row.group = group
					row.name = "name ${num}"
					row.value = num
					updater(row)
				}
			}
		}
		pt.finish(ds_source.writeRows)
	} 
	
	public void process() {
		declareSourceFields()
		if (!ds_source.existsFile()) generateSource()
		
		ds_all.field << new Field(name: "group", type: "INTEGER")
		ds_all.field << new Field(name: "count", type: "INTEGER")
		ds_all.field << new Field(name: "count_even", type: "INTEGER")
		ds_all.field << new Field(name: "name_min")
		ds_all.field << new Field(name: "name_max")
		ds_all.field << new Field(name: "value_sum", type: "INTEGER")
		ds_all.field << new Field(name: "value_min", type: "INTEGER")
		ds_all.field << new Field(name: "value_max", type: "INTEGER")
		
		def pt = new ProcessTime(name: "Transformation and save data")
		def count = new Flow().copy(source: ds_source, dest: ads)
		pt.finish(count)
	}
}
