package jdbc

import getl.data.*
import getl.jdbc.*
import getl.h2.*
import getl.proc.*
import getl.utils.*

import init.GenerateData

class ListTables extends Job {
	H2Connection h2 = new H2Connection(config: "h2")
	
	static main (args) {
		new ListTables().run()
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		println "H2 tables:"
		h2.retrieveDatasets().each { TableDataset dataset ->
			dataset.retrieveFields()
			println dataset
		}
		
		println "\nH2 views:"
		h2.retrieveDatasets([type: ["VIEW"]]).each { TableDataset dataset ->
			dataset.retrieveFields()
			println dataset
		}
		
		println "\nPUBLIC.DATA fields for query:"
		QueryDataset qData = new QueryDataset(connection: h2, query: "SELECT * FROM PUBLIC.DATA")
		def r = qData.rows(limit: 1)
		qData.field.each { println it }
	}

}
