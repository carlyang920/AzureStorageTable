using System;
using Microsoft.Azure.Cosmos.Table;

namespace Sample.AzureStorageTable.Test.Models
{
    public class TestEntityModel : TableEntity
    {
        public TestEntityModel()
        {
            PartitionKey = $"{DateTime.Now:yyyyMMddHH}";
            RowKey = Guid.NewGuid().ToString().Replace("-", "");
        }
        public string CustomerName { get; set; }
        public string Email { get; set; }
        public DateTime CurrentTime { get; set; } = DateTime.UtcNow;
    }
}
