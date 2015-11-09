package jdbc

import getl.jdbc.*
import getl.csv.CSVConnection
import getl.h2.*
import getl.proc.*
import getl.stat.ProcessTime
import getl.tfs.TDS
import getl.utils.*
import init.GenerateData

class UseBalancerConnection extends Job {
	// Connection to H2 database
	H2Connection h2 = new H2Connection(config: "h2")
	
	// Connection for file
	CSVConnection csvCon = new CSVConnection(config: "csv")
	
	// Destinition table (for generation run TableList example)
	TableDataset h2_table = new TableDataset(connection: h2, tableName: "DATA")
	
	/*
	Balancer balancer = new Balancer(checkTimeErrorServers: 2, servers: [
										[host: "localhost1", database: "getl_1"], 
										[host: "localhost", database: "getl_2"], 
										[host: "localhost", database: "getl_3"]])
	*/
	Balancer balancer = new Balancer(config: "h2mem")
	
	static main(args) {
		new UseBalancerConnection().run()
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		h2_table.retrieveFields()
		def servers = [] 
		
		(1..balancer.servers.size()).each {
			def server = new TDS(balancer: balancer)
			server.connected = true
			sleep 2000
			balancer.servers[0]."host" = "localhost"
			println "connected to server: ${server.currentConnectURL()}"
			servers << server
		}
		println "-------------"
		println "balancer status:"
		balancer.servers.each { println it }
		println()
		
		servers.each { TDS server -> 
			server.connected = false
			println "disconnected from: server ${server.currentConnectURL()}" 
		}
		println "-------------"
		println "balancer status:"
		balancer.servers.each { println it }
	}

}
