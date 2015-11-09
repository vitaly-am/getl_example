package threads

import getl.proc.Executor
import getl.proc.Job
import getl.stat.ProcessTime
import java.util.concurrent.atomic.AtomicInteger

class Run100000Threads extends Job {
	AtomicInteger count = new AtomicInteger(0)

	@Override
	public void process() {
		def pt = new ProcessTime(name: "Run 100000 process for 10 threads")
		new Executor().runMany(100000, 10) { num ->
			count.addAndGet(1)
		}
		pt.finish(count.get())
	}

	static main(args) {
		new Run100000Threads().run(args)
	}

}
