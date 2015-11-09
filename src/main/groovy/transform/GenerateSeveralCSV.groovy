package transform

import getl.proc.*
import getl.h2.*
import getl.jdbc.*
import getl.utils.*
import getl.stat.*

import init.GenerateData

class GenerateSeveralCSV extends Job {
	H2Connection db = new H2Connection(config: "h2")
	TableDataset tableMain = new TableDataset(connection: db, tableName: "MAIN")
	TableDataset tableChild = new TableDataset(connection: db, tableName: "CHILD")
	
	static main(args) {
		new GenerateSeveralCSV().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}

	@Override
	public void process() {
		db.startTran()
		tableChild.truncate()
		tableMain.truncate()
		db.commitTran()
		
		def pt = new ProcessTime(name: "Write rows")
		new Flow().writeAllTo(dest: [main: tableMain, child: tableChild]) { updater ->
			(1..10000).each { id_main ->
				Map row = [:]
				row.id = id_main
				row.name = "Value ${id_main}"
				updater("main", row)
				
				(1..100).each { num_child ->
					Map crow = [:]
					crow.main_id = id_main
					crow.num = num_child
					updater("child", crow)
				}
			}
		}
		pt.finish(tableMain.updateRows)
		
		Logs.Info("Save ${tableMain.updateRows}/${tableChild.updateRows} rows")
	}
}
