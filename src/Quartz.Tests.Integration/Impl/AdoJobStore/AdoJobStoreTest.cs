using System.Collections.Specialized;
using System.Threading.Channels;

using NUnit.Framework;
using Quartz.Impl.Matchers;
using Quartz.Tests.Integration.Impl;

namespace Quartz.Tests.Integration
{
    
    public class AdoJobStoreTest
    {
        private static readonly Dictionary<string, string> dbConnectionStrings = new Dictionary<string, string>();
        protected readonly string provider;
        protected readonly string serializerType;

        private const string Barrier = "BARRIER";
        private const string DateStamps = "DATE_STAMPS";
        
        public static string[] GetSerializerTypes() => new[] {"json", "binary"};

        [DisallowConcurrentExecution]
        [PersistJobDataAfterExecution]
        public class TestStatefulJob : IJob
        {
            public ValueTask Execute(IJobExecutionContext context)
            {
                return default;
            }
        }

        public class TestJob : IJob
        {
            public ValueTask Execute(IJobExecutionContext context)
            {
                context.JobDetail.JobDataMap.Put("testjobdata",2);
                return default;
            }
        }

        private IScheduler sched;
        private static readonly TimeSpan testTimeout = TimeSpan.FromSeconds(125);
        

        static AdoJobStoreTest()
        {
            dbConnectionStrings["PostgreSQL"] = TestConstants.PostgresConnectionString;
        }
        
        
        [OneTimeSetUp]
        public void TestPostgreSql()
        {
            NameValueCollection properties = new NameValueCollection();
            properties["quartz.jobStore.driverDelegateType"] = "Quartz.Impl.AdoJobStore.PostgreSQLDelegate, Quartz";
            RunAdoJobStoreTest("Npgsql", "PostgreSQL", "json", properties)
                .ConfigureAwait(false).GetAwaiter().GetResult();
        }
        
        protected async ValueTask RunAdoJobStoreTest(string dbProvider,
            string connectionStringId,
            string serializerType,
            NameValueCollection extraProperties,
            bool clustered = true)
        {
            var config = SchedulerBuilder.Create("instance_one", "TestScheduler");
            config.UseDefaultThreadPool(x =>
            {
                x.MaxConcurrency = 10;
            });
            config.MisfireThreshold = TimeSpan.FromSeconds(60);

            config.UsePersistentStore(store =>
            {
                store.UseProperties = false;
                store.PerformSchemaValidation = true;
                
                store.UseGenericDatabase(dbProvider, db =>
                    db.ConnectionString = dbConnectionStrings[connectionStringId]
                );

                if (serializerType == "json")
                {
                    store.UseJsonSerializer(j =>
                    {
                        j.AddCalendarSerializer<CustomCalendar>(new CustomCalendarSerializer());
                    });
                }
                else
                {
                    store.UseBinarySerializer();
                }
            });

            if (extraProperties != null)
            {
                foreach (string key in extraProperties.Keys)
                {
                    config.SetProperty(key, extraProperties[key]);
                }
            }

            // Clear any old errors from the log
            //testLoggerHelper.ClearLogs();

            // First we must get a reference to a scheduler
            sched = await config.BuildScheduler();
        }

        
        
        [Test]
        public async Task TestBasicStorageFunctions()
        {
            await sched.Clear();

            // test basic storage functions of scheduler...
            IJobDetail job = JobBuilder.Create<TestJob>()
                .WithIdentity("pj1")
                .StoreDurably(true)
                .PersistJobDataAfterExecution(true)
                .UsingJobData("testjobdata",1)
                .Build();
            
            ITrigger trigger = TriggerBuilder.Create()
                .WithIdentity("pt1")
                .ForJob(job)
                .StartNow()
                .Build();

            await sched.Start();
            
            await sched.ScheduleJob(job, trigger);

            await sched.TriggerJob(job.Key);
            
            
            while((await sched.GetTriggerState(trigger.Key)) != TriggerState.None);

            job = await sched.GetJobDetail(new JobKey("pj1"));
            
            Assert.AreEqual(job.JobDataMap["testjobdata"],2);
            

            await sched.Shutdown();
        }


        protected string CreateSchedulerName(string name)
        {
            return $"{name}_Scheduler_{provider}_{serializerType}";
        }
    }
}