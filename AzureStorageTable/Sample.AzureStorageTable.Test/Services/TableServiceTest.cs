using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Sample.AzureStorageTable.Interfaces;
using Sample.AzureStorageTable.Models;
using Sample.AzureStorageTable.Services;
using Sample.AzureStorageTable.Test.Models;

namespace Sample.AzureStorageTable.Test.Services
{
    [TestClass]
    public class TableServiceTest
    {
        private readonly ITableService<TestEntityModel> _service;

        public TableServiceTest()
        {
            _service = new TableService<TestEntityModel>(new TableConfigInfo()
            {
                AzureStorageConnectionString = @"[Storage connection]",
                TableName = @"tabletest",
                ProxyUrl = string.Empty
            });
        }

        [TestMethod]
        [Description("Test entity insertion")]
        public void InsertAsyncTest()
        {
            var entity = new TestEntityModel()
            {
                CustomerName = "TestName",
                Email = "test@testmail.com"
            };

            try
            {
                _service.InsertAsync(entity).Wait();

                var result = _service.GetEntityAsync(entity).Result;

                Assert.IsTrue(null != result);
            }
            catch (Exception e)
            {
                Debug.WriteLine($"{e}");
                Assert.IsTrue(false);
            }
            finally
            {
                _service.DeleteAsync(entity).Wait();
            }
        }

        [TestMethod]
        [Description("Test entity deletion")]
        public void DeleteAsyncTest()
        {
            var entity = new TestEntityModel()
            {
                CustomerName = "TestName",
                Email = "test@testmail.com"
            };

            try
            {
                _service.InsertAsync(entity).Wait();
                _service.DeleteAsync(entity).Wait();

                var result = _service.GetEntityAsync(entity).Result;

                Assert.IsTrue(null == result);
            }
            catch (Exception e)
            {
                Debug.WriteLine($"{e}");
                Assert.IsTrue(false);
            }
        }

        [TestMethod]
        [Description("Test entity insertion and replacement")]
        public void InsertOrReplaceAsyncTest()
        {
            var entity = new TestEntityModel()
            {
                CustomerName = "TestName",
                Email = "test@testmail.com"
            };

            try
            {
                var testResult = false;

                //Insert
                _service.InsertOrReplaceAsync(entity).Wait();

                entity = _service.GetEntityAsync(entity).Result;

                if (null != entity)
                {
                    testResult = true;

                    entity.CustomerName += "Replace";

                    //Replace
                    _service.InsertOrReplaceAsync(entity).Wait();

                    entity = _service.GetEntityAsync(entity).Result;

                    testResult = null != entity && entity.CustomerName.EndsWith("Replace");
                }

                Assert.IsTrue(testResult);
            }
            catch (Exception e)
            {
                Debug.WriteLine($"{e}");
                Assert.IsTrue(false);
            }
            finally
            {
                _service.DeleteAsync(entity).Wait();
            }
        }

        [TestMethod]
        [Description("Test entity batch insertion")]
        public void BatchInsertAsyncTest()
        {
            var count = 0;
            var list = new List<TestEntityModel>();
            var entity = new TestEntityModel()
            {
                CustomerName = "TestName",
                Email = "test@testmail.com"
            };

            try
            {
                do
                {
                    count += 1;

                    list.Add(new TestEntityModel()
                    {
                        CustomerName = $"{entity.CustomerName}{count}",
                        Email = $"{entity.Email}{count}",
                        CurrentTime = DateTime.UtcNow
                    });

                    Thread.Sleep(20);
                } while (1001 >= count);

                _service.BatchInsertAsync(list).Wait();

                var startTimeUtc = list.Min(p => p.CurrentTime);
                var endTimeUtc = list.Max(p => p.CurrentTime);

                var result = _service.GetEntitiesByTime("CurrentTime", startTimeUtc, endTimeUtc).ToList();

                Assert.IsTrue(list.Count <= result.Count);
            }
            catch (Exception e)
            {
                Debug.WriteLine($"{e}");
                Assert.IsTrue(false);
            }
            finally
            {
                _service.BatchDeleteAsync(list).Wait();
            }
        }

        [TestMethod]
        [Description("Test querying single entity")]
        public void GetEntityAsyncTest()
        {
            var entity = new TestEntityModel()
            {
                CustomerName = "TestName",
                Email = "test@testmail.com"
            };

            try
            {
                _service.InsertAsync(entity).Wait();

                var result = _service.GetEntityAsync(entity);

                Assert.IsTrue(null != result);
            }
            catch (Exception e)
            {
                Debug.WriteLine($"{e}");
                Assert.IsTrue(false);
            }
            finally
            {
                _service.DeleteAsync(entity).Wait();
            }
        }

        [TestMethod]
        [Description("Test querying all entities in table")]
        public void GetAllAsyncTest()
        {
            var startTimeUtc = DateTimeOffset.UtcNow;
            var count = 0;
            var list = new List<TestEntityModel>();
            var entity = new TestEntityModel()
            {
                CustomerName = "TestName",
                Email = "test@testmail.com"
            };

            try
            {
                do
                {
                    count += 1;

                    list.Add(new TestEntityModel()
                    {
                        CustomerName = $"{entity.CustomerName}{count}",
                        Email = $"{entity.Email}{count}"
                    });
                } while (101 >= count);

                _service.BatchInsertAsync(list).Wait();

                var result = _service.GetAllAsync().Result.ToList();

                Assert.IsTrue(count <= result.Count);
            }
            catch (Exception e)
            {
                Debug.WriteLine($"{e}");
                Assert.IsTrue(false);
            }
            finally
            {
                _service.BatchDeleteAsync(list).Wait();
            }
        }

        [TestMethod]
        [Description("Test querying entities in specific time period")]
        public void GetEntitiesByTimeTest()
        {
            var count = 0;
            var list = new List<TestEntityModel>();
            var entity = new TestEntityModel()
            {
                CustomerName = "TestName",
                Email = "test@testmail.com"
            };

            try
            {
                do
                {
                    count += 1;

                    list.Add(new TestEntityModel()
                    {
                        CustomerName = $"{entity.CustomerName}{count}",
                        Email = $"{entity.Email}{count}",
                        CurrentTime = DateTime.UtcNow
                    });

                    Thread.Sleep(50);
                } while (101 >= count);

                _service.BatchInsertAsync(list).Wait();

                var startTimeUtc = list.Min(p => p.CurrentTime);
                var endTimeUtc = list.Max(p => p.CurrentTime);

                var result = _service.GetEntitiesByTime("CurrentTime", startTimeUtc, endTimeUtc).ToList();

                Assert.IsTrue(count <= result.Count);
            }
            catch (Exception e)
            {
                Debug.WriteLine($"{e}");
                Assert.IsTrue(false);
            }
            finally
            {
                _service.BatchDeleteAsync(list).Wait();
            }
        }

        [TestMethod]
        [Description("Test batch deleting entities")]
        public void BatchDeleteAsyncTest()
        {
            var startTimeUtc = DateTimeOffset.UtcNow;
            var count = 0;
            var list = new List<TestEntityModel>();
            var entity = new TestEntityModel()
            {
                CustomerName = "TestName",
                Email = "test@testmail.com"
            };

            try
            {
                do
                {
                    count += 1;

                    list.Add(new TestEntityModel()
                    {
                        CustomerName = $"{entity.CustomerName}{count}",
                        Email = $"{entity.Email}{count}"
                    });
                } while (101 >= count);

                _service.BatchInsertAsync(list).Wait();

                _service.BatchDeleteAsync(list).Wait();

                var result = _service.GetEntitiesByTime("CurrentTime", startTimeUtc, DateTime.UtcNow).ToList();

                Assert.IsTrue(0 >= result.Count);
            }
            catch (Exception e)
            {
                Debug.WriteLine($"{e}");
                Assert.IsTrue(false);
            }
        }

        [TestMethod]
        [Description("Test deleting table")]
        public void DeleteTableAsyncTest()
        {
            const string tableName = "tableForTestDeletion";
            TableService<TestEntityModel> serviceForTest1 = null;

            try
            {
                serviceForTest1 = new TableService<TestEntityModel>(
                    new TableConfigInfo()
                    {
                        AzureStorageConnectionString =
                            @"[storage connection]",
                        TableName = tableName,
                        ProxyUrl = string.Empty
                    }
                );

                var count = 0;

                do
                {
                    count += 1;

                    if (!serviceForTest1.CheckTable(tableName)) continue;

                    serviceForTest1.DeleteTableAsync().Wait();

                    break;
                } while (100 >= count);

                Assert.IsTrue(!serviceForTest1.CheckTable(tableName));
            }
            catch (Exception e)
            {
                Debug.WriteLine($"{e}");
                Assert.IsTrue(false);
            }
        }

        [TestMethod]
        [Description("Test checking table")]
        public void CheckTableTest()
        {
            try
            {
                Assert.IsTrue(_service.CheckTable(@"tabletest"));
            }
            catch (Exception e)
            {
                Debug.WriteLine($"{e}");
                Assert.IsTrue(false);
            }
        }
    }
}
