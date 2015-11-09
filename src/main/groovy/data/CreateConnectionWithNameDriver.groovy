package data

import getl.proc.Job
import getl.utils.*
import getl.data.*

import init.GenerateData

class CreateConnectionWithNameDriver extends Job {
	@Override
	public void init () {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}

	@Override
	public void process() {
		def con = Connection.CreateConnection(connection: "getl.csv.CSVConnection", config: "csv")
		def data = Dataset.CreateDataset(dataset: "getl.csv.CSVDataset", connection: con, fileName: "data.csv", autoSchema: true)
		
		Logs.Info("Connection class: ${con.getClass().name}")
		Logs.Info("Dataset class: ${data.getClass().name}")
		def rows = data.rows()
		Logs.Info("${rows.size()} in data file")
	}

	static main(args) {
		new CreateConnectionWithNameDriver().run(args)
	}

}
