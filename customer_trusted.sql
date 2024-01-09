CREATE EXTERNAL TABLE IF NOT EXISTS `test`.`customer_trusted` (
  `serialNmber` string,
  `shareWithPublicAsOfDate` bigint,
  `birthDay` string,
  `registrationDate` bigint,
  `shareWithResearchAsOfDate` bigint,
  `customerName` string,
  `shareWithFriendsAsOfDate` bigint,
  `email` string,
  `lastUpdateDate` bigint,
  `phone` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://udacity-bucket-example-course/Customer/trusted/'
TBLPROPERTIES ('classification' = 'json');