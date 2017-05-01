using System;
using System.Configuration;
using System.Data;
using System.Threading;
using Amazon;
using Amazon.Redshift;
using Npgsql;

namespace AwsRedshiftHelloWorld
{
    internal class Program
    {
        private static RegionEndpoint _awsRegion;
        private static int _millisecondsToSleepBetweenQueries;
        private static bool _queryInALoop;

        static void Main()
        {
            _queryInALoop = Convert.ToBoolean(ConfigurationManager.AppSettings["QueryInALoop"]);
            _millisecondsToSleepBetweenQueries = Convert.ToInt32(ConfigurationManager.AppSettings["MillisecondsToSleepBetweenQueries"]);
            _awsRegion = RegionEndpoint.GetBySystemName(ConfigurationManager.AppSettings["AWS.Region"]);

            //reference: https://dotnetcodr.com/2015/03/26/using-amazon-redshift-with-the-aws-net-api-part-4-code-beginnings/
            DescribeRedShiftClusters();

            RunLoop();
        }

        //Just runs a query in a loop
        private static void RunLoop()
        {
            //reference: http://www.npgsql.org/doc/index.html
            using (var connection = new NpgsqlConnection(FetchRedshiftConnectionString()))
            {
                connection.Open();

                while (true)
                {
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = connection;

                        if (_queryInALoop)
                        {
                            // Retrieve all rows
                            cmd.CommandText = "select avg(someint) as someintavg,count(someint) as someintcount, max(whenwasit) as latest from foo";
                            using (var reader = cmd.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    Console.WriteLine($"{DateTime.Now:s} avg int: {reader["someintavg"]} count: {reader["someintcount"]} latest data: {reader["latest"]}");
                                }
                            }
                            Thread.Sleep(TimeSpan.FromMilliseconds(_millisecondsToSleepBetweenQueries));
                        }
                        else
                        {
                            Console.WriteLine("Enter string value:");
                            var bar = Console.ReadLine();

                            // Insert some data
                            cmd.CommandText = "INSERT INTO foo (bar, someint, whenwasit) VALUES (@bar, @someint, @whenwasit)";
                            cmd.Parameters.Add(new NpgsqlParameter { DbType = DbType.String, ParameterName = "bar", Value = bar });
                            cmd.Parameters.Add(new NpgsqlParameter { DbType = DbType.Int32, ParameterName = "someint", Value = DateTime.Now.Ticks % 1000 });
                            cmd.Parameters.Add(new NpgsqlParameter { DbType = DbType.DateTime, ParameterName = "whenwasit", Value = DateTime.UtcNow });
                            cmd.ExecuteNonQuery();

                            Console.WriteLine($"Successful insert {DateTime.Now:s}");
                        }
                    }
                }
            }
        }

        //helper to fetch the connection string for Redshift
        private static string FetchRedshiftConnectionString()
        {
            return new NpgsqlConnectionStringBuilder
            {
                Host = ConfigurationManager.AppSettings["AWS.Redshift.Host"],
                Port = Convert.ToInt32(ConfigurationManager.AppSettings["AWS.Redshift.Port"]),
                Username = ConfigurationManager.AppSettings["AWS.Redshift.Username"],
                Password = ConfigurationManager.AppSettings["AWS.Redshift.Password"],
                Database = ConfigurationManager.AppSettings["AWS.Redshift.Database"],
                ServerCompatibilityMode = ServerCompatibilityMode.Redshift // note: only to avoid https://github.com/npgsql/npgsql/issues/853
            }.ConnectionString;
        }

        public static void DescribeRedShiftClusters()
        {
            using (var redshiftClient = new AmazonRedshiftClient(_awsRegion))
            {
                var describeClustersResponse = redshiftClient.DescribeClusters();
                var redshiftClusters = describeClustersResponse.Clusters;
                foreach (var cluster in redshiftClusters)
                {
                    Console.WriteLine($"Cluster id: {cluster.ClusterIdentifier}");
                    Console.WriteLine($"Cluster status: {cluster.ClusterStatus}");
                    Console.WriteLine($"Cluster creation date: {cluster.ClusterCreateTime}");
                    Console.WriteLine($"Cluster DB name: {cluster.DBName}");
                }
            }
        }
    }
}
