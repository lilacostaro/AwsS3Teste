using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace AwsS3Teste
{
    public class S3Client
    {
        private readonly IAmazonS3 _s3Client;

        public S3Client(string serviceURL, string accessKey, string secretKey, RegionEndpoint region)
        {
            var config = new AmazonS3Config();
            config.RegionEndpoint = region;

            var isDevelopment = true;
            if (isDevelopment)
            {
                config.ServiceURL = serviceURL;
                config.ForcePathStyle = true;
            }
            else
            {
                config.RegionEndpoint = region;
            }

            _s3Client = new AmazonS3Client(accessKey, secretKey, config);
        }

        public async Task<string> InitiateMultipartUploadAsync(string bucketName, string keyName)
        {
            var initiateRequest = new InitiateMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = keyName
            };

            var response = await _s3Client.InitiateMultipartUploadAsync(initiateRequest);
            return response.UploadId;
        }

        public async Task<PartETag> UploadPartAsync(string bucketName, string keyName, string uploadId, int partNumber, MemoryStream memoryStream)
        {
            
            var uploadPartRequest = new UploadPartRequest
            {
                BucketName = bucketName,
                Key = keyName,
                UploadId = uploadId,
                PartNumber = partNumber,
                PartSize = memoryStream.Length,
                InputStream = memoryStream
            };

            var response = await _s3Client.UploadPartAsync(uploadPartRequest);
            return new PartETag
            {
                PartNumber = partNumber,
                ETag = response.ETag
            };
        }

        public async Task CompleteMultipartUploadAsync(string bucketName, string keyName, string uploadId, List<PartETag> partETags)
        {
            var completeRequest = new CompleteMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = keyName,
                UploadId = uploadId,
                PartETags = partETags
            };

            await _s3Client.CompleteMultipartUploadAsync(completeRequest);
        }

        public async Task AbortMultipartUploadAsync(string bucketName, string keyName, string uploadId)
        {
            var abortRequest = new AbortMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = keyName,
                UploadId = uploadId
            };

            await _s3Client.AbortMultipartUploadAsync(abortRequest);
        }

        public async Task UploadStreamInPartsAsync(string bucketName, string keyName, Stream stream, int partSize)
        {
            var uploadId = await InitiateMultipartUploadAsync(bucketName, keyName);
            var partETags = new List<PartETag>();
            int partNumber = 1;

            try
            {
                var buffer = new byte[partSize];
                int bytesRead;
                while ((bytesRead = await stream.ReadAsync(buffer, 0, partSize)) > 0)
                {
                    using (var partStream = new MemoryStream(buffer, 0, bytesRead))
                    {
                        var partETag = await UploadPartAsync(bucketName, keyName, uploadId, partNumber, partStream);
                        partETags.Add(partETag);
                    }

                    partNumber++;
                }

                await CompleteMultipartUploadAsync(bucketName, keyName, uploadId, partETags);
            }
            catch (Exception ex)
            {
                await AbortMultipartUploadAsync(bucketName, keyName, uploadId);
                throw new Exception("An error occurred during multipart upload. The upload was aborted.", ex);
            }
        }

        public async Task UploadObjectAsync(string bucketName, string keyName, string content)
        {
            var putRequest = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = keyName,
                ContentBody = content
            };

            await _s3Client.PutObjectAsync(putRequest);
        }

        public async Task<string> UploadStreamAsync(string bucketName, string keyName, Stream stream)
        {
            var putRequest = new PutObjectRequest
            {
                BucketName = bucketName,
                Key = keyName,
                InputStream = stream
            };

            await _s3Client.PutObjectAsync(putRequest);

            return keyName;
        }

        public async Task ListObjectsAsync(string bucketName)
        {
            var request = new ListObjectsRequest
            {
                BucketName = bucketName,
            };
            var response = await _s3Client.ListObjectsAsync(request);
            foreach (var entry in response.S3Objects)
            {
                Console.WriteLine($"Key: {entry.Key}, Size: {entry.Size}");
            }
        }

        public async Task<string> GetObjectContentAsync(string bucketName, string keyName)
        {
            var request = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = keyName
            };
            using (var response = await _s3Client.GetObjectAsync(request))
            using (var responseStream = response.ResponseStream)
            using (var reader = new System.IO.StreamReader(responseStream))
            {
                return await reader.ReadToEndAsync();
            }
        }

        public async Task DeleteObjectAsync(string bucketName, string keyName)
        {
            var deleteRequest = new DeleteObjectRequest
            {
                BucketName = bucketName,
                Key = keyName
            };

            await _s3Client.DeleteObjectAsync(deleteRequest);
        }

        public async Task<MemoryStream> DownloadObjectAsync(string bucketName, string keyName)
        {
            var request = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = keyName
            };

            var fileContent = new MemoryStream();

            var response = await _s3Client.GetObjectAsync(request);
            await response.ResponseStream.CopyToAsync(fileContent);

            return fileContent;
        }

        public async Task<bool> ObjectExistsAsync(string bucketName, string keyName)
        {
            try
            {
                var request = new GetObjectMetadataRequest
                {
                    BucketName = bucketName,
                    Key = keyName
                };
                var response = await _s3Client.GetObjectMetadataAsync(request);
                return true;
            }
            catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return false;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);

                throw;
            }
        }

        public async Task<string> GeneratePreSignedUrl(string bucketName, string keyName, TimeSpan expiryDuration)
        {
            var request = new GetPreSignedUrlRequest
            {
                BucketName = bucketName,
                Key = keyName,
                Expires = DateTime.UtcNow.Add(expiryDuration)
            };

            var signedUrl = await _s3Client.GetPreSignedURLAsync(request);

            return signedUrl;
        }

    }
}
