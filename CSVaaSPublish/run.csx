#r "Microsoft.WindowsAzure.Storage"

using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

public static void Run(Stream triggerBlob, string name, string listBlobIn, CloudBlockBlob listBlobOut, Stream outputBlob, TraceWriter log)
{
    // publish "latest"
    triggerBlob.CopyTo(outputBlob);

    // add original item to archive "all" list
    CSVList list;
    if(!String.IsNullOrEmpty(listBlobIn))
    {
        list = JsonConvert.DeserializeObject<CSVList>(listBlobIn);
    }
    else{
        list = new CSVList();
    }

    list.LatestUpdate = DateTime.UtcNow;
    list.Items.Add(name);

    listBlobOut.UploadText(JsonConvert.SerializeObject(list));
    listBlobOut.Properties.ContentType = "application/json";
    listBlobOut.SetProperties();
}

public class CSVList
{
    public CSVList() 
    {
        Items = new List<string>();
    }

    public DateTime LatestUpdate { get;set; }
    public List<string> Items { get;set; }    
}