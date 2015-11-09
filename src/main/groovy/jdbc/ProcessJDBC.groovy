package jdbc

import getl.jdbc.*
import getl.h2.*
import getl.proc.*
import getl.stat.ProcessTime
import getl.utils.*

import init.GenerateData

class ProcessJDBC extends Job {
	// Connection to H2 database
	H2Connection h2 = new H2Connection(config: "h2", fetchSize: 50)
	
	// Destinition table (for generation run TableList example)
	TableDataset h2_table = new TableDataset(connection: h2, tableName: "DATA")
	
	static main(args) {
		new ProcessJDBC().run()
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		ProcessTime.SetLogLevelDefault("INFO")
		
		int max_id = 0
		int count_original = 0
		int count_recovery = 0
		BigDecimal max_value_1_2
		Date max_time

		def pt = new ProcessTime(name: "EACH ROW")
		long count = new Flow().process(
						source: h2_table, 
						source_where: "source_id > 0",
						source_order: ["flag", "dt"]) { row ->
			if (max_id < row.source_id) max_id = row.source_id
			if (max_time < row.dt) max_time = row.dt
			if (!row.flag) count_original++ else count_recovery++
			BigDecimal sumValue_1_2 = row.value_1 + row.value_2
			if (max_value_1_2 < sumValue_1_2) max_value_1_2 = sumValue_1_2
		}
		
		pt.finish(count)
		println "Count: $count, max id: ${max_id}, max time: ${max_time}, even: ${count_recovery}, max value_1_2: ${max_value_1_2}"
		println ""
		
		if (Config.content."vars"?."init" != null) return
		
		def pta = new ProcessTime(name: "ALL ROWS")
		def rows = h2_table.rows(
						where: "source_id > 0",
						order: ["flag", "dt"],
						fetchSize: 1000)
		
		pta.finish(rows.size())
	}

}
