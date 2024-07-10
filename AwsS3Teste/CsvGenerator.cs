using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;

namespace AwsS3Teste;

public class CsvGenerator
{
    private readonly CultureInfo BrazilianCultureInfo = new CultureInfo("pt-BR");

    public async Task<MemoryStream> Generate<TRow, TClassMap>(IEnumerable<TRow> rows, bool isFirstPart, CancellationToken cancellationToken)
        where TClassMap : ClassMap<TRow>
    {
        var config = new CsvConfiguration(BrazilianCultureInfo)
        {
            HasHeaderRecord = false,
        };

        var memoryStream = new MemoryStream();
        await using (var writer = new StreamWriter(memoryStream, leaveOpen: true))
        await using (var csvWriter = new CsvWriter(writer, config))
        {
            if (isFirstPart)
            {
                csvWriter.Context.RegisterClassMap<TClassMap>();
                csvWriter.WriteHeader<TRow>();
                await csvWriter.NextRecordAsync();
            }
            
            await csvWriter.WriteRecordsAsync(rows, cancellationToken);
            await csvWriter.FlushAsync();
        }

        memoryStream.Seek(0, SeekOrigin.Begin);
        return memoryStream;
    }
}
