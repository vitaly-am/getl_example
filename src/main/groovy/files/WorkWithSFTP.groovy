package files

import getl.files.*
import getl.proc.Job
import getl.utils.*

/**
 * For get ssh key run:
 * ssh-keyscan -t rsa <IP>
 */

class WorkWithSFTP extends Job {
	@Override
	public void process() {
		def host = args."host"
		def login = args."login"
		def password = args."password"
		def knownHosts = args."hosts"
		def localDir = args."dir"
		def rootDir = args."root"
		assert host != null && login != null && password != null && knownHosts != null && localDir != null && rootDir != null
		
		SFTPManager sftp = new SFTPManager(server: host, login: login, password: password, knownHostsFile: knownHosts,
											localDirectory: localDir, rootPath: rootDir, 
											aliveInterval: 1, aliveCountMax: 3, noopTime: 1,
											scriptHistoryFile: "c:/tmp/WorkWithSFTP.txt",
											threadBuildList: 8, threadFilesCount: 500)
		
		sftp.connect()
		sftp.changeDirectory('/tmp')
		
		sftp.createLocalDir("workwithsftp", false)
		sftp.changeLocalDirectory("workwithsftp")
		def testFile = new File("${sftp.currentLocalDir()}/1.txt")
		if (!testFile.exists()) testFile << "TEST SFTP WORK"
		
		println "*** cd to test ***"
		if (!sftp.existsDirectory("test")) sftp.createDir("test") 
		sftp.changeDirectory("test")
		println "current directory: ${sftp.currentDir()}"
		sftp.upload("1.txt")
		
		println "*** test files *** "
		sftp.list("*.txt").each { println it }
		
		println "*** download 1.txt ***"
		sftp.download("1.txt")
		
		def f1 = "${sftp.currentLocalDir()}/1.txt"
		def f2 = "${sftp.currentLocalDir()}/2.txt"
		FileUtils.CopyToFile(f1, f2)
		
		println "*** upload 2.txt ***"
		sftp.upload("2.txt")
		sftp.removeLocalFile("2.txt")
		
		println "*** list files *** "
		sftp.listDir(null).each { println it }
		
		println "*** Build list *** "
		sftp.buildList(maskFile: "*") { println it; true }
		
		def out = new StringBuilder()
		def err = new StringBuilder()
		sftp.changeDirectoryUp()
		println "RES: ${sftp.command('cat test/1.txt', out, err)}"
		println "OUT: ${out.toString()}"
		if (err.length() > 0) System.err.write(err.toString())
		
		sftp.disconnect()
	}

	static main(args) {
		new WorkWithSFTP().run(args)
	}

}
