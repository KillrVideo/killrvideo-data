COPY default_keyspace.test_vectors_8d (id, name, vec) 
FROM 'test-copy-space-format.csv' 
WITH HEADER=TRUE 
AND ERRFILE='copy_errors.err'
AND MAXATTEMPTS=1
AND CHUNKSIZE=1
AND INGESTRATE=10
AND MAXBATCHSIZE=1;
