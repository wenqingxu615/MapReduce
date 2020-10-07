Customize these paths for your environment.
hadoop.root
local.input
local.output
aws.bucket.name
aws.input
aws.output

run the program on aws:
1.put the input file inside the input folder
	I have not add the input file in the tar package. Before running the program, add
it to the 'input' folder.

2.make upload-input-aws
	upload the input file to s3
3.make aws
	remember to change the bucket name
4.make download-output-aws
	download the output files to local