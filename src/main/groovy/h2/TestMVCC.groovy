package h2

import getl.data.Field
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.proc.Executor
import getl.proc.Flow
import getl.proc.Job
import getl.stat.ProcessTime
import getl.tfs.TDS
import getl.utils.Logs
import getl.utils.GenerationUtils
import getl.utils.SynchronizeObject

class TestMVCC extends Job {
	static countRows = 10000
	static batchSize = 100
	static threads = 50
	static mvcc = true
	static multi_threaded = false
	
	TDS h2 = new TDS(connectProperty: [MVCC: mvcc, MULTI_THREADED: multi_threaded], autoCommit: true)
	QueryDataset version = new QueryDataset(connection: h2, query: "select value as version from information_schema.settings where name ='info.VERSION';")
	TableDataset table = new TableDataset(connection: h2, tableName: "test")
	
	static main(args) {
		new TestMVCC().run(args)
	}

	@Override
	public void process() {
		def verRow = version.rows()
		println "H2 version: ${verRow[0].version}"
		
		h2.connected = true
		Logs.Fine("connect url: ${h2.currentConnectURL()}")
		
		table.field << new Field(name: "id", type: "INTEGER", isKey: true, isAutoincrement: true)
		table.field << new Field(name: "name", type: "STRING", isNull: false, length: 50)
		(1..30).each { col ->
			table.field << new Field(name: "value_$col", type: "NUMERIC", length: 15, precision: 2)
		}
		table.create()
		
		def processTime = new ProcessTime(name: "Copy to table with $threads threads")
		def counts = new SynchronizeObject()
		
		new Executor().runMany(threads) { num ->
			def pt = new ProcessTime(name: "save rows with $num threads")
			
			TDS c = new TDS(h2.params)
			TableDataset t = new TableDataset(connection: c, tableName: table.tableName, field: table.field)
			
			def count = new Flow().writeTo(dest: t, dest_batchSize: batchSize) { updater ->
				(1..countRows).each { 
					def r = [:]
					r."name" = GenerationUtils.GenerateString(50)
					(1..30).each { col ->
						r."value_$col" = GenerationUtils.GenerateNumeric(15, 2)
					}
					updater(r)
				}
			}
			
			pt.finish(count)
			counts.addCount(count)
		}
		
		processTime.finish(counts.count)
	}
}
