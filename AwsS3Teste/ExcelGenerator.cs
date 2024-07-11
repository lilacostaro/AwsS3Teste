using ClosedXML.Excel;
using System.Globalization;

namespace AwsS3Teste;

public class ExcelGenerator
{
    private readonly CultureInfo BrazilianCultureInfo = new CultureInfo("pt-BR");

    private readonly string _sheetName = "Página1";

    public async Task<MemoryStream> Generate<TRow>(IEnumerable<TRow> rows, CancellationToken cancellationToken = default, bool isFirstPart = true)
    {
        using var workbook = new XLWorkbook();

        var worksheet = workbook.Worksheets.Add(_sheetName);

        worksheet.FirstCell().InsertTable(rows, false);
        worksheet.Columns().AdjustToContents();

        var memoryStream = new MemoryStream();

        workbook.SaveAs(memoryStream);
        memoryStream.Seek(0, SeekOrigin.Begin);

        return memoryStream;
    }
}
