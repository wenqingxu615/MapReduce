Customize these paths for your environment.
hadoop.root
local.input
local.output
aws.bucket.name
aws.input
aws.output

run the program locally:
1.put the input file inside the input folder
	I have put the two input files for k = 1000.
	The ranks of each page is saved as "pagerank.txt" and the edges are saved as "edges.txt"
2.make upload-input-aws
	upload the input file to s3
3.make aws
	remember to change the bucket name
4.make download-output-aws
	download the output files to local