Customize these paths for your environment.
hadoop.root
local.input
local.output
aws.bucket.name
aws.input
aws.output

run the program locally:
1.put the input file inside the input folder
	I have not add the input file in the tar package. Before running the program, add
it to the 'input' folder.
2.make switch-standalone
	switch to standalone mode
3.make local

Locally run this program is OK on my pc. However, if yours has limited memory it may cause overflow. Anyway, it can run on the AWS.

=============================================================================

run the program on aws:
1.put the input file inside the input folder
	Ignore this step if you have already done.
2.make upload-app-aws
	upload the input file to s3
3.make aws
	remember to change the bucket name
4.make download-output-aws
	download the output files to local