package utils

import getl.csv.*
import getl.data.*
import getl.jdbc.*
import getl.h2.*
import getl.proc.*
import getl.utils.*

import init.GenerateData

class ReadConfig extends Job {
	static void main (args) {
		new ReadConfig().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		println "Example path: ${Config.content.examplePath}"
		
		Config.SetValue("section.subsection.param1", "value1")
		Config.content.section.subsection.param2 = "value2"
		
		def section_param = Config.FindSection("section")
		println "section params: ${section_param}"
		
		def subsection_param = Config.FindSection("section.subsection")
		println "section.subsection params: ${subsection_param}"
	}
}
