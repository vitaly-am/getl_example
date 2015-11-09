package utils

import getl.csv.*
import getl.data.*
import getl.proc.*
import getl.stat.*
import getl.utils.*

import init.GenerateData

class ConvertTextFile extends Job {
	CSVConnection csvCon = new CSVConnection(config: "csv")
	CSVDataset csvFile = new CSVDataset(connection: csvCon, fileName: "data.csv", autoSchema: true)

	static main(args) {
		new ConvertTextFile().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		def pt = new ProcessTime(name: "Convert file")
		def countRows = FileUtils.ConvertTextFile(csvFile.fullFileName(), "utf-8", false, 
													csvFile.connection.path + File.separator + "convert.txt", "utf-8", false, 
													[[type: "REGEXPR", old: "[ ]is[ ]", new: " _is_"], [old: "is", new: "as"], [old: ",", new: "\t"]])
		pt.finish(countRows)
	}

}
