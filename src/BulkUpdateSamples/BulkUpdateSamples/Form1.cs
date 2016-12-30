using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Transactions;
using System.Windows.Forms;
using Dapper;

namespace BulkUpdateSamples
{
    public partial class Form1 : Form
    {
        private static readonly string ConnectionString =
            File.ReadAllText(
                Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "connectionstring.txt"));

        public Form1()
        {
            this.InitializeComponent();
        }

        private static List<int> CreateShuffleIntList(int count)
        {
            var list = Enumerable.Range(0, count).ToList();

            var index = list.Count;
            var random = new Random(Guid.NewGuid().GetHashCode());

            while (index > 1)
            {
                index--;

                var randomPosition = random.Next(count);

                var tmp = list[index];
                list[index] = list[randomPosition];
                list[randomPosition] = tmp;
            }

            return list;
        }

        private void button1_Click(object sender, EventArgs e)
        {
            // 產生十萬筆資料
            var dt = new DataTable();
            dt.Columns.Add("Id", typeof(int));
            dt.Columns.Add("Text", typeof(string));

            for (var i = 0; i < 100000; i++)
            {
                var row = dt.NewRow();
                row["Id"] = i;
                row["Text"] = $"{Guid.NewGuid()}-{i:000000}";

                dt.Rows.Add(row);
            }

            var stopwatch = Stopwatch.StartNew();

            using (var tx = new TransactionScope())
            {
                using (var sql = new SqlConnection(ConnectionString))
                {
                    sql.Open();

                    using (var sqlBulkCopy = new SqlBulkCopy(sql))
                    {
                        sqlBulkCopy.DestinationTableName = "dbo.BulkTable";
                        sqlBulkCopy.WriteToServer(dt);
                    }
                }

                tx.Complete();
            }

            stopwatch.Stop();

            this.textBox1.AppendText($"{stopwatch.ElapsedMilliseconds}\r\n");
        }

        private void button2_Click(object sender, EventArgs e)
        {
            // 批次 Update
            var dt = new DataTable();
            dt.Columns.Add("Id", typeof(int));
            dt.Columns.Add("Text", typeof(string));

            for (var i = 0; i < 100000; i++)
            {
                var row = dt.NewRow();
                row["Id"] = i;
                row["Text"] = $"{i:000000}-{Guid.NewGuid()}-{DateTime.Now}";

                dt.Rows.Add(row);
            }

            var sb = new StringBuilder();
            sb.AppendLine(@"UPDATE [dbo].[BulkTable]");
            sb.AppendLine(@"  SET");
            sb.AppendLine(@"      [Text] = [ut].[Text]");
            sb.AppendLine(@"FROM [dbo].[BulkTable] [bt]");
            sb.AppendLine(@"     JOIN @UpdatedTable [ut] ON [bt].[Id] = [ut].[Id];");

            var stopwatch = Stopwatch.StartNew();

            using (var tx = new TransactionScope())
            {
                using (var sql = new SqlConnection(ConnectionString))
                {
                    sql.Execute(sb.ToString(), new { UpdatedTable = dt.AsTableValuedParameter("BulkTableType") });
                }

                tx.Complete();
            }

            stopwatch.Stop();

            this.textBox1.AppendText($"{stopwatch.ElapsedMilliseconds}\r\n");
        }

        private void button3_Click(object sender, EventArgs e)
        {
            // 批次 InsertOrUpdate
            var dt = new DataTable();
            dt.Columns.Add("Id", typeof(int));
            dt.Columns.Add("Text", typeof(string));

            var idList = CreateShuffleIntList(500000);

            for (var i = 0; i < 100000; i++)
            {
                // 隨機在五十萬內，取得新的 Id，代表要新增的資料。
                var row = dt.NewRow();
                row["Id"] = idList[i];
                row["Text"] = $"{idList[i]:000000}-{Guid.NewGuid()}-{DateTime.Now}";

                dt.Rows.Add(row);
            }

            var sb = new StringBuilder();
            sb.AppendLine(@"MERGE INTO [dbo].[BulkTable] [bt]");
            sb.AppendLine(@"USING @UpdatedTable [ut]");
            sb.AppendLine(@"ON [bt].[Id] = [ut].[Id]");
            sb.AppendLine(@"    WHEN MATCHED");
            sb.AppendLine(@"    THEN UPDATE SET [Text] = [ut].[Text]");
            sb.AppendLine(@"    WHEN NOT MATCHED");
            sb.AppendLine(@"    THEN INSERT VALUES ([ut].[Id], [ut].[Text]);");

            var stopwatch = Stopwatch.StartNew();

            using (var tx = new TransactionScope())
            {
                using (var sql = new SqlConnection(ConnectionString))
                {
                    sql.Execute(sb.ToString(), new { UpdatedTable = dt.AsTableValuedParameter("BulkTableType") });
                }

                tx.Complete();
            }

            stopwatch.Stop();

            this.textBox1.AppendText($"{stopwatch.ElapsedMilliseconds}\r\n");
        }
    }
}