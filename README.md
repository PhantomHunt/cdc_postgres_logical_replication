# cdc_postgres_logical_replication
This is implementation of CDC on postgresql database using pgoutput and test_decoding output plugins using python.

The underlying logic for pgoutput decoder is https://www.postgresql.org/docs/12/protocol-logicalrep-message-formats.html.

There are 2 scripts. One for test_decoding output plugin and other for pgoutput plugin.

The flow of code is as follows: 

1. Pgoutput
Postgresql DB -> postgresql WAL -> cdc_logical_replication_pgoutput.py will bring the encoded data from WAL -> Algorithm of utilities/message_decoder.py decode the message to string format -> Algorithm of utilities/message_formatter.py converts the operation messages [Insert('I'), Update('U'), Delete('D'), Truncate('T')] to JSON format.

  Raw decoded messages are converted to JSON so that it is more readable, understandable, contains column names and column values together as key-value and can easily be used further. In Raw decoded messages, column names are present only in Relation('R') message and it is generated by CDC WAL only when a table appears first time in current run or the table schema is altered. On the other hand, operation messages(I,U,D,T) has only the column values. Hence for more usability, formatter is created.

2. Test_decoding
Postgresql DB -> postgresql WAL -> cdc_logical_replication_test_decoding.py will bring the decoded data from WAL.


Keywords - cdc, WAL, postgresql, postgres, logical_replication, test_decoding, pgoutput, pgoutput_decoding, change_data_capture
