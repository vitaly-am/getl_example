package temporarydata

import getl.data.*
import getl.jdbc.*
import getl.proc.*
import getl.tfs.*

class TestTDSConnect extends Job {
	TDS con1 = new TDS()
	TDS con2 = new TDS()
	
	TableDataset table1 = new TableDataset(connection: con1, tableName: "table1") 

	@Override
	public void process() {
		table1.field << new Field(name: "id", type: "INTEGER", isNull: false, isKey: true)
		table1.field << new Field(name: "name", isNull: false, length: 50)
		table1.create()
		
		def ds1 = con1.retrieveDatasets()
		println ds1
		
		def ds2 = con2.retrieveDatasets()
		println ds2
	}

	static main(args) {
		new TestTDSConnect().run(args)
	}

}
