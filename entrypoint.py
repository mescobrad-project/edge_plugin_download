from mescobrad_edge.plugins.edge_plugin_download.models.plugin import EmptyPlugin, PluginActionResponse, PluginExchangeMetadata
from io import BytesIO

class GenericPlugin(EmptyPlugin):

    def action(self, input_meta: PluginExchangeMetadata = None) -> PluginActionResponse:
        import boto3
        from botocore.client import Config

        # Init client
        s3_local = boto3.resource('s3',
                                  endpoint_url= self.__OBJ_STORAGE_URL_LOCAL__,
                                  aws_access_key_id= self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                                  aws_secret_access_key= self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
                                  config=Config(signature_version='s3v4'),
                                  region_name=self.__OBJ_STORAGE_REGION__)

        # Download file from the local bucket to process
        bucket_local = s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__)
        obj_personal_data = bucket_local.objects.filter(Prefix="csv_data/", Delimiter="/")

        files_to_anonymize = [obj.key for obj in obj_personal_data]

        # Download data
        output = []
        for file_name in files_to_anonymize:
            f = BytesIO()
            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).download_fileobj(file_name, f)
            output.append(f.getvalue().decode())
        return PluginActionResponse("text/plain", output, files_to_anonymize)
