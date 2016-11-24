#r "Microsoft.WindowsAzure.Storage"

using System;
using System.Text;
using System.Text.RegularExpressions;
using CsvHelper;
using Microsoft.WindowsAzure.Storage.Blob;

public static void Run(Stream input, string name, CloudBlockBlob jsonFile, TraceWriter log)
{
    log.Info($"C# External trigger function processed file: " + name);

//   try{
    var boolRegex = new Regex(@"^([Tt]rue|[Ff]alse)$");
    var numericRegex = new Regex(@"^[\d\.,]*\d[\d\.,]*$");

    var reader = new StreamReader(input);
    var csv = new CsvReader(reader);
    csv.Configuration.HasHeaderRecord = false;

    var headers = new List<string>();
    var rows = new List<List<string>>();
    InspectedType[] types = null;
    int rowCount = 0;
    string stringValue;
    while(csv.Read())
    {
        if(rowCount == 0)
        {
            for(int i=0; csv.TryGetField<string>(i, out stringValue); i++) 
            {
                headers.Add(stringValue);
            }
            types = new InspectedType[headers.Count()];
            log.Info($"Headers: " + String.Join(",", headers));            
        }
        else
        {
            var row = new List<string>();
            for(int i=0; csv.TryGetField<string>(i, out stringValue); i++) 
            {
                row.Add(stringValue);
                if(rowCount == 1)
                {
                    if(boolRegex.IsMatch(stringValue))
                    {
                        types[i] = InspectedType.Boolean;
                    }
                    else if(numericRegex.IsMatch(stringValue))
                    {
                        types[i] = InspectedType.Number;
                    }
                    else
                    {
                        types[i] = InspectedType.String;
                    }
                }
                else
                {
                    if(types[i] == InspectedType.Boolean && !boolRegex.IsMatch(stringValue))
                    {
                        types[i] = InspectedType.String;
                    }
                    else if(types[i] == InspectedType.Number && !numericRegex.IsMatch(stringValue))
                    {
                        types[i] = InspectedType.String;
                    }
                }
            }
            rows.Add(row);
            log.Info($"Row $rowCount: " + String.Join(",", row));
        }
        rowCount++;
    }

    log.Info("Inspected Row Types: " + String.Join(",", types));

//    }
//    catch(Exception exc){
//        log.Info($"C# Exception while processing: $name: " + exc.Message);
//    }

    var time = DateTime.UtcNow.ToString("s");
    var typesDescription = "\"" + String.Join("\",\"", types) + "\"";
    var sb = new StringBuilder();
    sb.AppendLine("{");
    sb.AppendFormat("\"info\": {{ \"sourceFile\": \"{0}\", \"processedTime\": \"{1}\", \"types\": [{2}] }},",
                    name, time, typesDescription);

    sb.AppendLine("\"rows\": [");
    bool isFirstRow = true;
    int columnCount = headers.Count();
    foreach(var row in rows)
    {
        if(!isFirstRow)
        {
            sb.AppendLine(",");
        }
        isFirstRow = false;

        for(var i = 0; i < columnCount; i++)
        {
            var value = row[i];
            if(types[i] == InspectedType.Boolean)
            {
                value = row[i].ToLower();
            }
            else if(types[i] == InspectedType.Number)
            {
                value = row[i].Replace(",", "");
            }
            else
            {
                value = "\"" + row[i].Replace("\"","\\\"") + "\"";
            }

            sb.AppendFormat("{0}\"{1}\": {2}{3}",
                            i == 0 ? "{" : ", ", 
                            headers[i], 
                            value,
                            i == columnCount - 1 ? "}" : "");
        }
    }
    sb.AppendLine("]");
    sb.AppendLine("}");

    jsonFile.UploadText(sb.ToString());
    jsonFile.Properties.ContentType = "application/json";
    jsonFile.SetProperties();
}

public enum InspectedType
{
    String = 0,
    Number = 1,
    Boolean = 2    
}