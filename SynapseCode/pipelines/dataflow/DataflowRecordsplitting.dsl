source(output(
		sourceName as string,
		sourceVersion as string,
		device as string,
		type as string,
		unit as string,
		creationDate as string,
		startDate as string,
		endDate as string,
		value as double
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'delimited',
	fileSystem: 'datalake',
	folderPath: 'Bronze/apple_health_export/source/csv',
	fileName: 'Record.csv',
	columnDelimiter: ',',
	escapeChar: '\\',
	quoteChar: '\"',
	columnNamesAsHeader: true,
	qutoChar: '\"') ~> sourceRecords
DerivedColumn1 select(mapColumn(
		each(match(name!='sourceName'&&name!='sourceVersion'&&name!='device'))
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> Select1
sourceRecords derive(activityType = type) ~> DerivedColumn1
Select1 sink(allowSchemaDrift: true,
	validateSchema: false,
	format: 'parquet',
	filePattern:'activityrecords[n].parquet',
	truncate: true,
	umask: 0022,
	partitionBy('key',
		0,
		type
	)) ~> sink1