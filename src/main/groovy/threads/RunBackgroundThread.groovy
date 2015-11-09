package threads

import getl.proc.Executor
import getl.proc.Job

class RunBackgroundThread extends Job {

	@Override
	public void process() {
		def num = 0
		
		def executor = new Executor(waitTime: 100)
		executor.startBackground {
			println "Current number: ${num}"
		}
		
		(1..10).each {
			num++
			sleep 200
		}
		executor.stopBackground()
	}

	static main(args) {
		new RunBackgroundThread().run(args)
	}

}
