using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace Sample.AzureStorageTable.Interfaces
{
    public interface ITableService<T> where T : TableEntity, new()
    {
        Task InsertAsync(T entity);
        Task InsertOrReplaceAsync(T entity);
        Task BatchInsertAsync(IEnumerable<T> entities, bool isCheckExists = false);
        Task<T> GetEntityAsync(T entity);
        Task<IEnumerable<T>> GetAllAsync();
        IEnumerable<T> GetEntitiesByTime(string queryPropertyName, DateTimeOffset startUtcTime, DateTimeOffset endUtcTime);
        Task DeleteAsync(T entity);
        Task BatchDeleteAsync(IEnumerable<T> entities);
        Task DeleteTableAsync();
        bool CheckTable(string tableName);
    }
}
