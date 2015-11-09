package utils

import getl.utils.*

class DenormalizeColumn {

	static main(args) {
		def text = "id=1,name=text value,numeric=100.23"
		Map row = TransformUtils.DenormalizeColumn(text, ",", "=")
		row.each { field, value ->
			println "${field}: ${value}"
		}
	}

}
