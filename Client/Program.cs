using Amazon;
using Amazon.S3.Model;
using AwsS3Teste;
using CsvHelper.Configuration;
using System.Buffers;
using System.Text;

namespace Client
{
    class Program
    {
        private const string ServiceURL = "http://localhost:4566";
        private const string AccessKey = "test";
        private const string SecretKey = "test";
        private static readonly RegionEndpoint Region = RegionEndpoint.GetBySystemName("us-west-2");

        private const string BucketName = "bucket-test";
        private const string KeyNameBytes = "csv-teste-multiparte.csv";
        private const int PartSize = 6 * 1024 * 1024; // 6 MB
        private static int _currentIndex = 1;
        private static bool _writeHeader = true;
        private static int _bufferPosition = 0;

        static async Task Main(string[] args)
        {
            var s3Service = new S3Client(ServiceURL, AccessKey, SecretKey, Region);

            Console.WriteLine("Uploading CSV to S3 in parts bytes method...");
            await UploadCsvInPartsAsync(s3Service, BucketName, KeyNameBytes);

            Console.WriteLine("Listing objects...");
            await s3Service.ListObjectsAsync(BucketName);

            Console.WriteLine("Checking if object exists...");
            bool exists = await s3Service.ObjectExistsAsync(BucketName, KeyNameBytes);
            Console.WriteLine($"Object exists: {exists}");

            if (exists)
            {
                Console.WriteLine("Generating pre-signed URL...");
                var url = await s3Service.GeneratePreSignedUrl(BucketName, KeyNameBytes, TimeSpan.FromMinutes(300));
                Console.WriteLine($"Pre-signed URL: {url}");
            }
            else
            {
                Console.WriteLine("File does not exist...");
            }
        }

        private static async Task UploadCsvInPartsAsync(S3Client s3Service, string bucketName, string keyName)
        {
            var uploadId = await s3Service.InitiateMultipartUploadAsync(bucketName, keyName);
            var partETags = new List<PartETag>();
            int partNumber = 1;

            try
            {
                while (true)
                {
                    var buffer = GenerateCsvPartStream(out bool isLastPart);
                    if (buffer == null)
                    {
                        break;
                    }

                    using MemoryStream partStream = new(buffer, 0, _bufferPosition);
                    var partETag = await s3Service.UploadPartAsync(bucketName, keyName, uploadId, partNumber, partStream);
                    partETags.Add(partETag);
                    Console.WriteLine($"Part {partNumber} uploaded. ETag: {partETag.ETag}");
                    partNumber++;

                    if (isLastPart)
                    {
                        break;
                    }
                }

                await s3Service.CompleteMultipartUploadAsync(bucketName, keyName, uploadId, partETags);
                Console.WriteLine("Multipart upload completed successfully.");
            }
            catch (Exception ex)
            {
                await s3Service.AbortMultipartUploadAsync(bucketName, keyName, uploadId);
                throw new Exception("An error occurred during multipart upload. The upload was aborted.", ex);
            }
        }
        
        private static MemoryStream CreateCsvStream()
        {
            var csv = new StringBuilder();
            csv.AppendLine("Id,Name,Age");
            csv.AppendLine("1,John Doe,30");
            csv.AppendLine("2,Jane Smith,25");
            csv.AppendLine("3,Bob Johnson,35");

            var byteArray = Encoding.UTF8.GetBytes(csv.ToString());
            var stream = new MemoryStream(byteArray);
            return stream;
        }

        private static byte[] GenerateCsvPartStream(out bool isLastPart)
        {
            var csv_content = new List<MyData>();
            var csvGenerator = new CsvGenerator();
            isLastPart = false;
            byte[] buffer = ArrayPool<byte>.Shared.Rent(PartSize);
            _bufferPosition = 0;

            try
            {
                while (_currentIndex <= 200000)
                {
                    var data = new MyData()
                    {
                        ID = _currentIndex,
                        Name = $"Joana {_currentIndex}",
                        SobreNome = $"Da Silva Souza {_currentIndex}",
                        Vacina = "Sim e Não Quem sabe",
                        Escolaridade = "Ensino medio e quer fazer faculdade",
                        Tipo = "Qualquer um, só quero encher a memoria",
                        Endereco = "Rua dos Bobos numero 0",
                        Age = 20 + (_currentIndex % 30),
                        Count = _currentIndex + 2
                    };
                    csv_content.Add(data);

                    _currentIndex++;

                    // Verifica se já percorreu todos os itens
                    if (_currentIndex > 200000)
                    {
                        isLastPart = true;
                    }

                    
                    if (_currentIndex % 2000 == 0)
                    {
                        using (var memoryStreamb = csvGenerator.Generate<MyData, MyDataMap>(csv_content, _writeHeader, CancellationToken.None).Result)
                        {
                            int bytesRead = memoryStreamb.ReadAsync(buffer.AsMemory(_bufferPosition, (int)memoryStreamb.Length)).Result;
                            _bufferPosition += bytesRead;
                        }
                        Console.WriteLine($"Current Index: {_currentIndex}");
                        Console.WriteLine($"Buffer Position: {_bufferPosition}");
                        _writeHeader = false;
                        csv_content.Clear();
                        if (_bufferPosition >= PartSize)
                        {
                            return buffer;
                        }
                    }
                }
                
                using (var memoryStreamb = csvGenerator.Generate<MyData, MyDataMap>(csv_content, _writeHeader, CancellationToken.None).Result)
                {
                    int bytesToRead = (int)Math.Min(PartSize - _bufferPosition, memoryStreamb.Length);
                    int bytesRead = memoryStreamb.ReadAsync(buffer.AsMemory(_bufferPosition, bytesToRead)).Result;
                    _bufferPosition += bytesRead;
                }
                return buffer;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error generating CSV part: {ex.Message}");
                return null;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        public class MyData
        {
            public int ID { get; set; }
            public string Name { get; set; }
            public string SobreNome { get; set; }
            public string Vacina { get; set; }
            public string Escolaridade { get; set; }
            public string Tipo { get; set; }
            public string Endereco { get; set; }
            public int Age { get; set; }
            public int Count { get; set; }
        }

        public sealed class MyDataMap : ClassMap<MyData>
        {
            public MyDataMap()
            {
                Map(m => m.ID).Name("ID");
                Map(m => m.Name).Name("Name");
                Map(m => m.SobreNome).Name("SobreNome");
                Map(m => m.Vacina).Name("Vacina");
                Map(m => m.Escolaridade).Name("Escolaridade");
                Map(m => m.Tipo).Name("Tipo");
                Map(m => m.Endereco).Name("Endereço");
                Map(m => m.Age).Name("Age");
                Map(m => m.Count).Name("Count");
            }
        }
    }
}