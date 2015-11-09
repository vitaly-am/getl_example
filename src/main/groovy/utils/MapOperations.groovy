package utils

import getl.csv.*
import getl.data.*
import getl.jdbc.*
import getl.h2.*
import getl.proc.*
import getl.utils.*

import init.GenerateData

class MapOperations extends Job {
	static void main (args) {
		new MapOperations().run(args)
	}
	
	void init() {
		Config.path = GenerateData.ExamplePath
		Config.fileName = GenerateData.ConfigFile
	}
	
	void process () {
		def m1 = [node1: [subnode1: '${valueSubNode}', subnode2: ['${valueItem1 * map.node2}', '${valueItem2 * map.node2}']], node2: 1]
		
		// Clone map
		def m2 = MapUtils.DeepCopy(m1)
		
		// Change value from clone map
		m2.node1.subnode1 = '${valueSubNodeNew}'
		m2.node2 = 2
		
		println "Original map: $m1"
		println "Clone map: $m2"
		assert m1 != m2
		
		println ""
		def vars = [valueSubNode: "Sub value", valueSubNodeNew: "Sub value new", valueItem1: 100, valueItem2: 200]
		println "vars: $vars"
		def mv1 = MapUtils.EvalMacroValues(m1, vars + [map: m1])
		def mv2 = MapUtils.EvalMacroValues(m2, vars + [map: m2])
		
		println "Eval values of original map: $mv1"
		println "Eval values of clone map: $mv2"
	}
}
