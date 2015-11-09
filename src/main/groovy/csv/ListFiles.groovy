package csv;

import getl.data.*
import getl.proc.*
import getl.csv.*
import getl.utils.*

import init.GenerateData

public class ListFiles extends Job {
	CSVConnection files = new CSVConnection(config: "csv")

	static main (args) {
		new ListFiles().run()
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		println ">>> All files:"
		files.retrieveObjects().each { println it.path }

		println "\n>>> Only CSV files:"
		files.retrieveObjects(mask: "(?i).*[.]CSV").each { println it.path }
		
		println "\n>>> Recursive files:"
		files.retrieveObjects(recursive: true).each { println it.path }
		
		println "\n>>> Sub directories:"
		files.retrieveObjects(type: "DIR").each { println it.path }
		
		println "\n>>> Recursive subdirectories:"
		files.retrieveObjects(type: "DIR", recursive: true).each { println it.path }

		println "\n>>> File from parent directory:"
		files.retrieveObjects(directory: "..").each { println it.path }
	}	
}
