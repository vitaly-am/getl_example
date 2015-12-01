package files

import getl.proc.*
import getl.utils.*
import getl.data.Field
import getl.files.*
import getl.h2.*
import getl.jdbc.*

import init.GenerateData

class FileList extends Job {
	H2Connection conHistory = new H2Connection(config: "h2", autoCommit: true)
	TableDataset dsHistory = new TableDataset(connection: conHistory, tableName: "test_files")
	
	static main(args) {
		new FileList().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}

	@Override
	public void process() {
		FileManager files = new FileManager(rootPath: Config.content.examplePath, localDirectory: "${Config.content.examplePath}/files")
		// Connect to file manager
		files.connect()
		
		def sayCurDir = {
			println "\nCurrent directory: ${files.currentDir}:"
		}
		
		// Printing csv files from current directory
		sayCurDir()
		files.list() { Map file -> println file }
		
		// Change dir to CSV folder
		files.changeDirectory("CSV")
		
		
		sayCurDir()
		// Process list directory
		files.list("*.csv") { Map file -> println file }
		
		files.changeDirectoryToRoot()
		
		// Build recursive list files to fileList dataset
		files.buildList(path: new Path(mask: "{type}/{name}.{extension}", vars: [name: [lenMax: 10]]), recursive: true) { Map file ->
			!(file.type.toLowerCase() in ["files", "h2"] || (file.extension?.toLowerCase() in ["lck", "log"]))
		}
		
		sayCurDir()
		// Printing fileList
		files.fileList.eachRow { row -> println row }
		
		if (!dsHistory.exists) {
			files.AddFieldsToDS(dsHistory)
			dsHistory.field << new Field(name: "type", length: 50)
			dsHistory.field << new Field(name: "name", length: 128)
			dsHistory.field << new Field(name: "extension", length: 10)
			dsHistory.create()
		}
		
		sayCurDir()
		// Download files from fileList
		files.downloadFiles(story: dsHistory) { Map file ->
			println "Download ${file.filepath}/${file.filename}"
		}
		
		//conHistory.connected = false
		
		println "\nStore download history table:"
		dsHistory.eachRow { row -> println row }
	}
}
