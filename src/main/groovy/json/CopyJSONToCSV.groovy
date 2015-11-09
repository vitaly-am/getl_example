package json

import getl.csv.*
import getl.data.*
import getl.proc.*
import getl.json.*
import getl.stat.*
import getl.tfs.*
import getl.utils.*

import init.GenerateData

class CopyJSONToCSV extends Job {
	static CopyJSONToCSV job = new CopyJSONToCSV()
	
	JSONConnection con = new JSONConnection(config: "json")
	JSONDataset ds = new JSONDataset(connection: con, rootNode: ".", fileName: "events.json.gz", isGzFile: true, convertToList: true)

	CSVConnection csv = new CSVConnection(config: "csv")
	CSVDataset events = new CSVDataset(connection: csv, fileName: "json.events.csv.gz", isGzFile: true)
	CSVDataset services = new CSVDataset(connection: csv, fileName: "json.services.csv.gz", isGzFile: true, field: [new Field(name: "PARENT_ID", type: "INTEGER"), new Field(name: "NAME")])
	CSVDataset accums = new CSVDataset(connection: csv, fileName: "json.accums.csv.gz", isGzFile: true, 
										field: [new Field(name: "PARENT_ID", type: "INTEGER"), new Field(name: "ACCUM_ID"),
												new Field(name: "SCHEME_ID"), new Field(name: "ACCUM_VALUE", type: "INTEGER"), 
												new Field(name: "LAST_RESET", type: "DATETIME"), 
												new Field(name: "NEXT_RESET", type: "DATETIME")
												])
	
	static void main (args) {
		job.run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		ds.field << new Field(name: "SUBSCRIBER_ID", type: "INTEGER")
		ds.field << new Field(name: "EVENT_CAUSE", alias: "EVENT")
		ds.field << new Field(name: "TIMESTAMP", type: "DATETIME")
		ds.field << new Field(name: "SESSION_ID")
		ds.field << new Field(name: "IP")
		ds.field << new Field(name: "BS_ID")
		ds.field << new Field(name: "RULES")
		ds.field << new Field(name: "POLICYS", alias: "QOS_POLICY")
		ds.field << new Field(name: "SERVICES", type: "OBJECT")
		ds.field << new Field(name: "ACCUMS", type: "OBJECT")
		
		events.field = ds.field
		events.field.add(0, new Field(name: "ID", type: "INTEGER", isAutoincrement: true, isKey: true))
		events.removeField("SERVICES")
		events.removeField("ACCUMS")
		
		def pt = new ProcessTime(className: CopyJSONToCSV, log: "INFO")

		long count
		long id = 0
		new Flow().writeAllTo(dest: ["services": services, "accums": accums]) { writer ->
			count = new Flow().copy(source: ds, dest: events) { rowIn, rowOut ->
				id ++
				rowOut.id = id
				rowIn."services"?.each { struct ->
					def r = [:]
					r.parent_id = id
					r.name = struct?.NAME
					writer("services", r)
				}
				rowIn."accums"?.each { struct ->
					def r = [:]
					r.parent_id = id
					r.accum_id = struct?.ACCUM_ID
					r.scheme_id = struct?.SCHEME_ID
					r.accum_value = struct?.ACCUM_VALUE
					r.last_reset = DateUtils.ParseDate("yyyy-MM-dd HH:mm:ss", struct?.LAST_RESET)
					r.next_reset = DateUtils.ParseDate("yyyy-MM-dd HH:mm:ss", struct?.NEXT_RESET)
					writer("accums", r)
				}
			}
		}
		
		pt.finish(count)
	}
}
