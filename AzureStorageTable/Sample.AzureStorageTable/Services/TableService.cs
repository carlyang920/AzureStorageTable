using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;
using Sample.AzureStorageTable.Handler;
using Sample.AzureStorageTable.Interfaces;
using Sample.AzureStorageTable.Models;

namespace Sample.AzureStorageTable.Services
{
    public class TableService<T> : ITableService<T> where T : TableEntity, new()
    {
        private readonly TableConfigInfo _cloudTableConfigInfo;
        private readonly CloudStorageAccount _cloudStorageAccount;
        private CloudTableClient _cloudTableClient;
        private readonly CloudTable _cloudTable;

        public TableService(TableConfigInfo tableConfig)
        {
            _cloudTableConfigInfo = tableConfig;

            CloudStorageAccount.TryParse(_cloudTableConfigInfo.AzureStorageConnectionString, out _cloudStorageAccount);

            _cloudTableConfigInfo = tableConfig;
            _cloudTable = GetTableAsync(_cloudTableConfigInfo.TableName).Result;
        }

        /// <summary>
        /// Generate table client
        /// </summary>
        /// <returns></returns>
        private CloudTableClient GetCloudTableClient()
        {
            //If no proxy setting, don't use custom DelegatingHandler.
            if (!string.IsNullOrEmpty(_cloudTableConfigInfo.ProxyUrl))
            {
                var delegatingHandler = new TableProxyHandler(new WebProxy(_cloudTableConfigInfo.ProxyUrl, true));

                _cloudTableClient = _cloudStorageAccount.CreateCloudTableClient(new TableClientConfiguration
                {
                    RestExecutorConfiguration = new RestExecutorConfiguration
                    {
                        DelegatingHandler = delegatingHandler
                    }
                });
            }
            else
            {
                _cloudTableClient = _cloudStorageAccount.CreateCloudTableClient();
            }

            return _cloudTableClient;
        }

        /// <summary>
        /// Get table reference.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private async Task<CloudTable> GetTableAsync(string tableName)
        {
            var tableClient = GetCloudTableClient();

            // Create a table client for interacting with the table service 
            var table = tableClient.GetTableReference(tableName);

            await table.CreateIfNotExistsAsync();

            return table;
        }

        /// <summary>
        /// Get table result from table storage
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        private async Task<TableResult> GetTableResult(T entity)
        {
            var retrieveOperation = TableOperation.Retrieve<T>(entity.PartitionKey, entity.RowKey);
            var result = await _cloudTable.ExecuteAsync(retrieveOperation);

            return result;
        }

        /// <summary>
        /// Insert single entity into table.
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public async Task InsertAsync(T entity)
        {
            // Create the InsertOrReplace table operation
            var operation = TableOperation.Insert(entity);

            // Execute the operation.
            await _cloudTable.ExecuteAsync(operation);
        }

        /// <summary>
        /// If entity key exists, replace it. Otherwise, insert it.
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public async Task InsertOrReplaceAsync(T entity)
        {
            var tableResult = await GetTableResult(entity);
            var replaceEntity = (T)tableResult.Result;

            if (null != replaceEntity)
            {
                //Replace entity
                replaceEntity = entity;
                var insertOrReplaceOperation = TableOperation.InsertOrReplace(replaceEntity);
                await _cloudTable.ExecuteAsync(insertOrReplaceOperation);
            }
            else
            {
                //Insert entity
                var insertOrReplaceOperation = TableOperation.InsertOrReplace(entity);
                await _cloudTable.ExecuteAsync(insertOrReplaceOperation);
            }
        }

        /// <summary>
        /// Batch insert entities per 100 rows into table.
        /// </summary>
        /// <param name="entities">Data for insert</param>
        /// <param name="isCheckExists">Check data exists in table or not. If it exists, move it into inserted list and don't send it again.</param>
        /// <returns>If batch insertion failed since redundant data, it will add PartitionKey and RowKey into Exception.Data</returns>
        public async Task BatchInsertAsync(IEnumerable<T> entities, bool isCheckExists = false)
        {
            var inserted = new List<T>();
            var tableEntities = entities.ToList();

            //If data exists in Azure Table Storage, delete ti from list
            if (isCheckExists)
            {
                //If entity already exists, move it to inserted list.
                tableEntities.ForEach(p =>
                {
                    if (null == GetEntityAsync(p).Result) return;
                    inserted.Add(p);
                });

                //Remove the exists entities from inserting list
                tableEntities.RemoveAll(
                    p => inserted.Any(
                        item => Equals(item.PartitionKey, p.PartitionKey)
                                &&
                                Equals(item.RowKey, p.RowKey))
                );
            }

            var partitionKeys = tableEntities.Select(p => new { p.PartitionKey }).Distinct().ToList();

            //It can't have duplicate PartitionKey and RowKey in each batch insertion.
            foreach (var par in partitionKeys)
            {
                var batchOperation = new TableBatchOperation();
                var count = 0;
                var partitionEntities = tableEntities.Where(x => string.Equals(x.PartitionKey, par.PartitionKey, StringComparison.OrdinalIgnoreCase)).Distinct().ToList();

                foreach (var ety in partitionEntities)
                {
                    //Check Data exists or not
                    if (!batchOperation.Any(
                        p => 
                            string.Equals(p.Entity.PartitionKey, ety.PartitionKey, StringComparison.OrdinalIgnoreCase) && 
                            string.Equals(p.Entity.RowKey, ety.RowKey, StringComparison.OrdinalIgnoreCase)
                            ))
                    {
                        batchOperation.Insert(ety);

                        count += 1;
                    }

                    if (100 > count) continue;

                    try
                    {
                        await _cloudTable.ExecuteBatchAsync(batchOperation);

                        batchOperation.ToList().ForEach(p =>
                        {
                            inserted.Add((T)p.Entity);
                        });
                    }
                    catch (StorageException e)
                    {
                        if (e.RequestInformation.HttpStatusCode == 409 &&
                            e.RequestInformation.HttpStatusMessage.Contains("The specified entity already exists"))
                        {
                            var errors = e.RequestInformation.HttpStatusMessage.Split(':');
                            var index = Convert.ToInt32(2 <= errors.Length ? errors[0] : "0");

                            e.Data.Add("RedundantEntity", JsonConvert.SerializeObject(batchOperation[index].Entity));
                        }

                        e.Data.Add("Inserted", JsonConvert.SerializeObject(inserted));
                        e.Data.Add("Config", JsonConvert.SerializeObject(_cloudTableConfigInfo));

                        throw;
                    }
                    catch (Exception ex)
                    {
                        ex.Data.Add("Inserted", JsonConvert.SerializeObject(inserted));
                        ex.Data.Add("Config", JsonConvert.SerializeObject(_cloudTableConfigInfo));

                        throw;
                    }
                    finally
                    {
                        batchOperation.Clear();

                        count = 0;
                    }
                }

                if (0 >= count) continue;

                try
                {
                    //Insert the rest of entities
                    await _cloudTable.ExecuteBatchAsync(batchOperation);

                    batchOperation.ToList().ForEach(p => { inserted.Add((T) p.Entity); });
                }
                catch (StorageException e)
                {
                    if (e.RequestInformation.HttpStatusCode == 409 &&
                        e.RequestInformation.HttpStatusMessage.Contains("The specified entity already exists"))
                    {
                        var errors = e.RequestInformation.HttpStatusMessage.Split(':');
                        var index = Convert.ToInt32(2 <= errors.Length ? errors[0] : "0");

                        e.Data.Add("RedundantEntity", JsonConvert.SerializeObject(batchOperation[index].Entity));
                    }

                    e.Data.Add("Inserted", JsonConvert.SerializeObject(inserted));
                    e.Data.Add("Config", JsonConvert.SerializeObject(_cloudTableConfigInfo));

                    throw;
                }
                catch (Exception ex)
                {
                    ex.Data.Add("Inserted", JsonConvert.SerializeObject(inserted));
                    ex.Data.Add("Config", JsonConvert.SerializeObject(_cloudTableConfigInfo));

                    throw;
                }
                finally
                {
                    batchOperation.Clear();
                }
            }
        }

        /// <summary>
        /// Get single entity by partition key and row key.
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public async Task<T> GetEntityAsync(T entity)
        {
            var tableResult = await GetTableResult(entity);
            var result = (T)tableResult.Result;

            return result;
        }

        /// <summary>
        /// Get all entities of table.
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<T>> GetAllAsync()
        {
            var list = new List<T>();
            TableContinuationToken token = null;
            var query = new TableQuery<T>();

            do
            {
                var result = await _cloudTable.ExecuteQuerySegmentedAsync(query, token);
                token = result.ContinuationToken;
                list.AddRange(result.Results);
            } while (null != token);

            return list;
        }

        /// <summary>
        /// Get all entities by RowKey
        /// </summary>
        /// <param name="queryPropertyName"></param>
        /// <param name="startUtcTime"></param>
        /// <param name="endUtcTime"></param>
        /// <returns></returns>
        public IEnumerable<T> GetEntitiesByTime(string queryPropertyName, DateTimeOffset startUtcTime, DateTimeOffset endUtcTime)
        {
            var rangeQuery = new TableQuery<T>()
                .Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterConditionForDate(queryPropertyName, QueryComparisons.GreaterThanOrEqual, startUtcTime),
                        TableOperators.And,
                        TableQuery.GenerateFilterConditionForDate(queryPropertyName, QueryComparisons.LessThanOrEqual, endUtcTime))
                );

            return _cloudTable.ExecuteQuery(rangeQuery);
        }

        /// <summary>
        /// Delete entity.
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public async Task DeleteAsync(T entity)
        {
            var retrievedResult = await GetTableResult(entity);
            var deleteEntity = (T)retrievedResult.Result;
            if (deleteEntity != null)
            {
                var deleteOperation = TableOperation.Delete(deleteEntity);
                await _cloudTable.ExecuteAsync(deleteOperation);
            }
            else
            {
                throw new NullReferenceException("Cannot found entity");
            }
        }

        /// <summary>
        /// Batch delete entity.
        /// </summary>
        /// <param name="entities"></param>
        /// <returns></returns>
        public async Task BatchDeleteAsync(IEnumerable<T> entities)
        {
            var count = 0;
            var batchOperation = new TableBatchOperation();

            foreach (var ety in entities)
            {
                batchOperation.Delete(ety);

                count += 1;

                if (100 != count) continue;

                await _cloudTable.ExecuteBatchAsync(batchOperation);
                batchOperation.Clear();

                count = 0;
            }

            //Insert the rest of entities
            if (0 < count)
                await _cloudTable.ExecuteBatchAsync(batchOperation);
        }

        /// <summary>
        /// Delete table.
        /// Note:
        /// It can't create the same table after deleting a while.
        /// </summary>
        /// <returns></returns>
        public async Task DeleteTableAsync()
        {
            await _cloudTable.DeleteIfExistsAsync();
        }

        /// <summary>
        /// Check table exists or not.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public bool CheckTable(string tableName)
        {
            var result =  _cloudTableClient
                .ListTables().Count(p => string.Equals(tableName, p.Name));

            return 0 < result;
        }
    }
}
