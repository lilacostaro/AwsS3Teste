using Amazon;
using Amazon.S3.Model;
using AwsS3Teste;
using ClosedXML.Attributes;
using CsvHelper.Configuration;
using System.Buffers;

namespace Client
{
    class Program
    {
        private const string ServiceURL = "http://localhost:4566";
        private const string AccessKey = "test";
        private const string SecretKey = "test";
        private static readonly RegionEndpoint Region = RegionEndpoint.GetBySystemName("us-west-2");

        private const string BucketName = "bucket-test";
        private const string KeyNameExcel = "excel-teste.xlsx";
        private const string KeyNameExcelStream = "csv-teste-multiparte-1.csv";
        private const string KeyNameCsv = "csv-teste.csv";
        private const int PartSize = 10 * 1024 * 1024; // 10 MB
        private static int _currentIndex = 1;
        private static bool _writeHeader = true;
        private static int _bufferPosition = 0;
        private static bool _isLastPart = false;

        static async Task Main(string[] args)
        {
            var s3Service = new S3Client(ServiceURL, AccessKey, SecretKey, Region);

            Console.WriteLine("Uploading CSV to S3 in parts bytes method...");
            await UploadCsvInPartsAsync(s3Service, BucketName, KeyNameExcelStream);

            Console.WriteLine("Listing objects...");
            await s3Service.ListObjectsAsync(BucketName);

            Console.WriteLine("Checking if object exists...");
            bool exists = await s3Service.ObjectExistsAsync(BucketName, KeyNameExcelStream);
            Console.WriteLine($"Object exists: {exists}");

            if (exists)
            {
                Console.WriteLine("Generating pre-signed URL...");
                var url = await s3Service.GeneratePreSignedUrl(BucketName, KeyNameExcelStream, TimeSpan.FromMinutes(300));
                Console.WriteLine($"Pre-signed URL: {url}");
            }
            else
            {
                Console.WriteLine("File does not exist...");
            }

            //Console.WriteLine("Generating Excel file...");
            //var excelContent = await CreateExcelStream();
            //Console.WriteLine("Uploading Excel to S3...");
            //await s3Service.UploadStreamAsync(BucketName, KeyNameExcel, excelContent);

            //Console.WriteLine("Generating Excel file...");
            //var csvContent = await CreateCsvStream();
            //Console.WriteLine("Uploading Excel to S3...");
            //await s3Service.UploadStreamAsync(BucketName, KeyNameCsv, csvContent);

            //var excelUrl = await s3Service.GeneratePreSignedUrl(BucketName, KeyNameExcel, TimeSpan.FromMinutes(300));
            //Console.WriteLine($"Pre-signed URL: {excelUrl}");

            //var csvUrl = await s3Service.GeneratePreSignedUrl(BucketName, KeyNameCsv, TimeSpan.FromMinutes(300));
            //Console.WriteLine($"Pre-signed URL: {csvUrl}");
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
                    var buffer = GenerateCsvPartStream();
                    if (buffer == null)
                    {
                        break;
                    }

                    using MemoryStream partStream = new(buffer, 0, _bufferPosition);
                    var partETag = await s3Service.UploadPartAsync(bucketName, keyName, uploadId, partNumber, partStream);
                    partETags.Add(partETag);
                    Console.WriteLine($"Part {partNumber} uploaded. ETag: {partETag.ETag}");
                    partNumber++;

                    if (_isLastPart)
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

        private static async Task UploadExcelInPartsAsync(S3Client s3Service, string bucketName, string keyName)
        {
            var uploadId = await s3Service.InitiateMultipartUploadAsync(bucketName, keyName);
            var partETags = new List<PartETag>();
            int partNumber = 1;

            try
            {
                while (true)
                {
                    var partStream = await GenerateExcelPartStreamTest();
                    if (partStream == null)
                    {
                        break;
                    }

                    Console.WriteLine($"Uploading part: {partNumber}, with size: {partStream.Length}");
                    var partETag = await s3Service.UploadPartAsync(bucketName, keyName, uploadId, partNumber, partStream);
                    partETags.Add(partETag);
                    Console.WriteLine($"Part {partNumber} uploaded. ETag: {partETag.ETag}");
                    partNumber++;

                    if (_isLastPart)
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


        private static async Task<MemoryStream> CreateCsvStream()
        {
            var csv_content = new List<MyData>();
            var generator = new CsvGenerator();
            var currentIndex = 1;

            while (currentIndex <= 2000)
            {
                var data = new MyData()
                {
                    ID = _currentIndex,
                    Name = $"Joana {currentIndex}",
                    LastName = $"Da Silva Souza {currentIndex}",
                    Vacina = "Sim e Não Quem sabe",
                    Escolaridade = "Ensino medio e quer fazer faculdade",
                    Tipo = "Qualquer um, só quero encher a memoria",
                    Endereco = "Rua dos Bobos numero 0",
                    Age = 20 + (currentIndex % 30),
                    Count = currentIndex + 2
                };
                csv_content.Add(data);

                currentIndex++;
            }

            var memoryStream = await generator.Generate<MyData, MyDataMap>(csv_content, _writeHeader, CancellationToken.None);

            return memoryStream;
        }

        private static async Task<MemoryStream> CreateExcelStream()
        {
            var csv_content = new List<MyData>();
            var generator = new ExcelGenerator();
            var currentIndex = 1;

            while (currentIndex <= 2000)
            {
                var data = new MyData()
                {
                    ID = _currentIndex,
                    Name = $"Joana {currentIndex}",
                    LastName = $"Da Silva Souza {currentIndex}",
                    Vacina = "Sim e Não Quem sabe",
                    Escolaridade = "Ensino medio e quer fazer faculdade",
                    Tipo = "Qualquer um, só quero encher a memoria",
                    Endereco = "Rua dos Bobos numero 0",
                    Age = 20 + (currentIndex % 30),
                    Count = currentIndex + 2
                };
                csv_content.Add(data);

                currentIndex++;
            }

            var memoryStream = await generator.Generate<MyData>(csv_content, CancellationToken.None, _writeHeader);

            return memoryStream;
        }

        private static byte[] GenerateCsvPartStream()
        {
            var csv_content = new List<MyData>();
            var csvGenerator = new CsvGenerator();
            _isLastPart = false;
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
                        LastName = $"Da Silva Souza {_currentIndex}",
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
                        _isLastPart = true;
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

        private static byte[] GenerateExcelPartStream()
        {
            var csv_content = new List<MyData>();
            var generator = new ExcelGenerator();
            _isLastPart = false;
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
                        LastName = $"Da Silva Souza {_currentIndex}",
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
                        _isLastPart = true;
                    }


                    if (_currentIndex % 2000 == 0)
                    {
                        using (var memoryStreamb = generator.Generate<MyData>(csv_content, CancellationToken.None, _writeHeader).Result)
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

                using (var memoryStreamb = generator.Generate<MyData>(csv_content, CancellationToken.None, _writeHeader).Result)
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

        private static async Task<MemoryStream> GenerateExcelPartStreamTest()
        {
            var csv_content = new List<MyData>();
            var generator = new CsvGenerator();
            var memoryStream = new MemoryStream();
            _isLastPart = false;

            try
            {
                while (_currentIndex <= 200000)
                {
                    var data = new MyData()
                    {
                        ID = _currentIndex,
                        Name = $"Joana {_currentIndex}",
                        LastName = $"Da Silva Souza {_currentIndex}",
                        Vacina = "Sim e Não Quem sabe",
                        Escolaridade = "Ensino medio e quer fazer faculdade",
                        Tipo = "Qualquer um, só quero encher a memoria",
                        Endereco = "Rua dos Bobos numero 0",
                        Age = 20 + (_currentIndex % 30),
                        Count = _currentIndex + 2
                    };
                    csv_content.Add(data);

                    _currentIndex++;

                    if (_currentIndex > 200000)
                    {
                        _isLastPart = true;
                    }

                    if (_currentIndex % 2000 == 0)
                    {
                        using (var partStream = await generator.Generate<MyData, MyDataMap>(csv_content, _writeHeader, CancellationToken.None))
                        {
                            partStream.CopyTo(memoryStream);
                            csv_content.Clear();
                            _writeHeader = false;
                        }
                        Console.WriteLine($"Current Index: {_currentIndex}");
                        Console.WriteLine($"MemoryStream Size: {memoryStream.Length}");

                        if (memoryStream.Length >= PartSize)
                        {
                            return memoryStream;
                        }
                    }
                }

                if (csv_content.Count > 0)
                {
                    using (var partStream = await generator.Generate<MyData, MyDataMap>(csv_content, _writeHeader, CancellationToken.None))
                    {
                        partStream.CopyTo(memoryStream);
                    }
                }

                memoryStream.Position = 0;
                return memoryStream;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error generating Excel part: {ex.Message}");
                return null;
            }
        }

        public class MyData
        {
            [XLColumn(Header = "ID")]
            public int ID { get; set; }
            
            [XLColumn(Header = "Nome")]
            public string Name { get; set; }

            [XLColumn(Header = "Sobrenome")]
            public string LastName { get; set; }

            [XLColumn(Header = "Vacina")]
            public string Vacina { get; set; }

            [XLColumn(Header = "Escolaridade")]
            public string Escolaridade { get; set; }

            [XLColumn(Header = "Tipo")]
            public string Tipo { get; set; }

            [XLColumn(Header = "Endereço")]
            public string Endereco { get; set; }

            [XLColumn(Header = "Idade")]
            public int Age { get; set; }
            
            [XLColumn(Header = "Contador")]
            public int Count { get; set; }
        }

        public sealed class MyDataMap : ClassMap<MyData>
        {
            public MyDataMap()
            {
                Map(m => m.ID).Name("ID");
                Map(m => m.Name).Name("Nome");
                Map(m => m.LastName).Name("SobreNome");
                Map(m => m.Vacina).Name("Vacina");
                Map(m => m.Escolaridade).Name("Escolaridade");
                Map(m => m.Tipo).Name("Tipo");
                Map(m => m.Endereco).Name("Endereço");
                Map(m => m.Age).Name("Idade");
                Map(m => m.Count).Name("Contador");
            }
        }
    }
}