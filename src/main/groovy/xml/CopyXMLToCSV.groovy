package xml

import getl.proc.*
import getl.stat.*
import getl.xml.*
import getl.csv.*
import getl.data.*

class CopyXMLToCSV extends Job {
	XMLConnection source = new XMLConnection(path: "c:/temp/getl_example/xml", isGzFile: true)
	XMLDataset xml = new XMLDataset(connection: source, fileName: "test.xml.gz", rootNode: "measData.measInfo")
	
	CSVConnection dest = new CSVConnection(path: "c:/temp/getl_example/csv")
	CSVDataset csv = new CSVDataset(connection: dest, fileName: "test_xml.csv")

	public void process() {
		xml.with {
			attributeField << new Field(name: "version", alias: "fileHeader.@fileFormatVersion[0]")
			attributeField << new Field(name: "vendor", alias: "fileHeader.@vendorName[0]")
			attributeField << new Field(name: "sender", alias: "fileHeader.fileSender.@elementType[0]")
			attributeField << new Field(name: "beginTime", type: "DATETIME", alias: "fileHeader.measCollec.@beginTime[0]", format: "yyyy-MM-dd'T'HH:mm:ss")
			attributeField << new Field(name: "eNodeB", alias: "measData.managedElement.@userLabel[0]")
			
			field << new Field(name: "function", alias: "@measInfoId")
			field << new Field(name: "duration", alias: "repPeriod.@duration")
			field << new Field(name: "counters", alias: "measTypes.text()")
			field << new Field(name: "values", type: "OBJECT", alias: "measValue")
		}
		
		new Flow().process(source: xml) { row ->
			println "Function: ${row.function}, Duration: ${row.duration}"
			println row.counters
			row.values.each { v ->
				print v.@measObjLdn
				print "\t"
				println v.measResults.text()
			}
			println "-----------------"
			println ""
		}
		
		println xml.attributeValue
		
	}

	static main(args) {
		new CopyXMLToCSV().run()
	}

}
