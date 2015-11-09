package cachedata
 
import getl.proc.*
import getl.cache.*
import getl.data.Dataset
import getl.tfs.*
import getl.h2.*
import getl.jdbc.*
import getl.utils.*

import init.GenerateData

class WorkWithCache extends Job {
	H2Connection h2 = new H2Connection(config: "h2")
	CacheManager cm = new CacheManager(config: "h2mem")
	
	TableDataset data1 = new TableDataset(connection: h2, tableName: "DATA", onUpdateFields: { println "Read list of field from data1" })
	TableDataset data2 = new TableDataset(connection: h2, tableName: "DATA", onUpdateFields: { println "Read list of field from data2" })
	TableDataset data3 = new TableDataset(connection: h2, tableName: "DATA", cacheManager: cm, onUpdateFields: { println "Read list of field from data3" })
	
	CacheDataset cache1 = new CacheDataset(connection: cm, dataset: data1, liveTime: 20, onUpdateFields: { println "Read list of field from cache1" }, onUpdateData: { println "Update data from cache1" })
	CacheDataset cache2 = new CacheDataset(connection: cm, dataset: data2, liveTime: 20, onUpdateFields: { println "Read list of field from cache2" }, onUpdateData: { println "Update data from cache2" })

	static main(args) {
		new WorkWithCache().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	@Override
	public void process() {
		println data1.manualSchema
		listOfTables()
		
		listOfCacheConnections()
		listOfCacheObjects()
		
		println data1.manualSchema
		println "*** read cache1 from source dataset:"
		println cache1.rows(limit: 10, order: ["id"])
		println "Last retrieve time: ${cache1.cacheReaded}"
		println ""
		
		println "*** read cache2 from cache:"
		println "*** retrieve from cache:"
		println cache2.rows(limit: 10, order: ["id"])
		println "Last retrieve time: ${cache2.cacheReaded}"
		println ""
		
		// Sleep 20 second 
		sleep 20000
		
		println "*** read cache2 from source dataset:"
		println cache2.rows(limit: 10, order: ["id"])
		println "Last retrieve time: ${cache1.cacheReaded}"
		println ""
		
		println "*** read cache1 from cache:"
		println cache1.rows(limit: 10, order: ["id"])
		println "Last retrieve time: ${cache1.cacheReaded}"
		println ""
		
		println "*** reread data3 fields from metadata cache:"
		data3.field.clear()
		data3.retrieveFields()
		println "Fields: ${data3.field}"
	}
	
	void listOfTables () {
		println "*** Tables in schema:"
		cm.retrieveDatasets(schemaName: cm.objectsSchema).each { TableDataset dataset ->
			dataset.retrieveFields()
			println "DATASET: ${dataset}\nFIELDS: ${dataset.field}\n"
		}
		println "***\n"
	}
	
	void listOfCacheConnections () {
		println "*** List of cached connections:"
		cm.connections.eachRow { row -> println "ID: ${row.connectionid}, DRIVER: ${row.driver}, NAME: ${row.name}"}
		println "***\n"
	}
	
	void listOfCacheObjects () {
		println "*** List of cached objects:"
		cm.objects.eachRow { row -> println "NAME: ${row.name}, READED: ${row.readed}, UPDATED: ${row.updated}" }
		println "***\n"
	}

}
