package init

import getl.proc.*
import getl.utils.*
import csv.*
import jdbc.*
import transform.*

class GenerateData extends Job {
	// Example directory
	public static final ExamplePath = "${FileUtils.UserHomeDir()}/getl/example"
	
	// Config file name
	public static final ConfigFile = "getl.example.conf"

	static main(args) {
		new GenerateData().run(args)
	}
	
	void process() {
		// Create example directory
		FileUtils.ValidPath(ExamplePath)
	
		// Save configuration	
		Logs.Info("Create config file")
		generateConfig()
		
		Config.SetValue("vars.init", true)
		
		// Generate CSV File
		Logs.Info("Create CSV file")
		new GenerateCSV().process()
		
		// Valid save CSV file
		Logs.Info("Valid CSV file")
		new ProcessCSV().process()
		
		// Generate H2 objects
		Logs.Info("Create H2 objects")
		new CreateJDBCObjects().process()
		
		// Load CSV file to H2 table DATA
		Logs.Info("Load CSV to DATA table")
		new CopyCSVToJDBC().process()
		
		// Valid save rows by table
		Logs.Info("Valid save rows by table")
		new ProcessJDBC().process()
		
		// Generate source data from transformation
		Logs.Info("Create source data for transformation examples")
		new CopyCSVToCSVWithTransformation().generateSource()
	}
	
	void generateConfig() {
		Config.SetValue("examplePath", ExamplePath)
		
		Config.SetValue("statistic.level", "FINE")
		Config.SetValue("log.file", "${ExamplePath}/getl_example.log")
		
		Config.SetValue("connections.csv.path", "${ExamplePath}/CSV")
		
		Config.SetValue("connections.h2.connectURL", "jdbc:h2:${ExamplePath}/H2/example")
		Config.SetValue("connections.h2.login", "SA")
		
		Config.SetValue("connections.h2mem.connectURL", "jdbc:h2:mem:getl")
		Config.SetValue("connections.h2mem.login", "SA")
		
		Config.SetValue("connections.json.path", "${ExamplePath}/JSON")
		Config.SetValue("connections.xml.path", "${ExamplePath}/XML")
		
		Config.SetValue("balancers.h2mem.checkTimeErrorServers", 2)
		Config.SetValue("balancers.h2mem.servers", [
							["host": "localhost1", "database": "getl_1"],
							["host": "localhost" , "database": "getl_2"],
							["host": "localhost" , "database": "getl_3"]
						])
		
		Config.SaveConfig("${ExamplePath}/${ConfigFile}")
	}
}
