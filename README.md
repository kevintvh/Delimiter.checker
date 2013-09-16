Delimiter.checker
===============

This will run a map only job to determine if the correct number of columns are in each row.  

The cmd looks like the following:

DelimiterChecker <inputPath> <delimiterChar> <expectedColumnCount> <ValidRecordOutputPath> <OptionalInValidRecordOutputPath>

Example of filtering only good records: 
  DelimiterChecker tmp/file.txt , 5 tmp/goodrecords 

Example of filtering good record but also output bad records:   
  DelimiterChecker tmp/file.txt , 5 tmp/goodRecords tmp/badRecords

Remember if your delimiter is | use \\| in the command line because \\| is a special character