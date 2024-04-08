import boto3
import sys

client = boto3.client('s3')
bucket = '<bucket-name>'
prefix = str(sys.argv[1])
paginator = client.get_paginator('list_object_versions')
operation_parameters = {'Bucket': bucket, 'Prefix': prefix}
page_iterator = paginator.paginate(**operation_parameters, PaginationConfig={'PageSize': 200})

version_list = []
delete_marker_list = []

print("Deleting :" + prefix)


for page in page_iterator:
    if 'Versions' in page:
        for version in page['Versions']:
            version_list.append({'Key': version['Key'],'VersionId': version['VersionId']})
    if 'DeleteMarkers' in page:
        for delete_marker in page['DeleteMarkers']:
            delete_marker_list.append({'Key': delete_marker['Key'],'VersionId': delete_marker['VersionId']})

print(delete_marker_list)
     
for i in range(0, len(version_list), 1000):
    response = client.delete_objects(Bucket=bucket,Delete={'Objects': version_list[i:i+1000]})
    print(response)

for i in range(0, len(delete_marker_list), 1000):
    response = client.delete_objects(Bucket=bucket,Delete={'Objects': delete_marker_list[i:i+1000]})
    print(response)



