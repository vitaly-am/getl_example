package utils

import getl.proc.Job
import getl.utils.Logs
import getl.utils.Config

import init.GenerateData

class LogWork extends Job {

	@Override
	public void process() {
		Logs.Fine("TEST FINE")
		Logs.Info("TEST INFO")
		Logs.Warning("TEST WARNING")
		Logs.Severe("TEST SEVERE")
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}

	static main(args) {
		new LogWork().run(args)
	}

}
