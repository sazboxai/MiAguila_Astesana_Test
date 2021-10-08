import os
import boto3
from botocore import exceptions
from gevent import monkey

monkey.patch_all()


class AWS:
    def __init__(self, access_key_id, secret_access_key):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

    def get_files_name(self, bucket, subdir, file_extension):
        s3 = boto3.client('s3', aws_access_key_id=self.access_key_id, aws_secret_access_key=self.secret_access_key)
        data = s3.list_objects(Bucket=bucket, Prefix=subdir)
        file_names = []
        for file in data['Contents']:
            file = file['Key']
            if file_extension in file:
                file_names.append(file)
        return file_names

    def csv_from_s3(self, bucket, subdir, file_name, local_folder='data/'):
        if not os.path.exists(local_folder):
            os.makedirs(local_folder, exist_ok=False)
            print("The new directory was created!")

        if os.path.exists(local_folder + file_name):
            print(f"The file {file_name} already exists!")

        else:
            try:
                s3 = boto3.resource('s3', aws_access_key_id=self.access_key_id,
                                    aws_secret_access_key=self.secret_access_key)
                s3.Bucket(bucket).download_file(subdir + file_name, local_folder + file_name)
                print(f"The file {file_name} was download!")

                file = open(local_folder + file_name + ".txt", "w")
                file.write("processed lines:0")
                file.close()
                print(f"The file {file_name}.txt was created!")

            except exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    print("The object does not exist on S3 Bucket.")
                else:
                    raise ValueError("An generic error occur")
