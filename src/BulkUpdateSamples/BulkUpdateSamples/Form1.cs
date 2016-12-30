using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Reflection;
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

            var stopwatch = Stopwatch.StartNew();

            using (var tx = new TransactionScope())
            {
                using (var sql = new SqlConnection(ConnectionString))
                {
                    sql.Execute(
                        "dbo.BulkUpdateBulkTable",
                        new { UpdatedTable = dt },
                        commandType: CommandType.StoredProcedure);
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

            var random = new Random(Guid.NewGuid().GetHashCode());

            var idList = new List<int>();

            for (var i = 0; i < 100000; i++)
            {
                // 隨機在五十萬內，取得新的 Id，代表要新增的資料。
                var id = random.Next(500000);

                if (idList.Contains(id))
                {
                    i--;
                    continue;
                }

                idList.Add(id);

                var row = dt.NewRow();
                row["Id"] = id;
                row["Text"] = $"{id:000000}-{Guid.NewGuid()}-{DateTime.Now}";

                dt.Rows.Add(row);
            }

            var stopwatch = Stopwatch.StartNew();

            using (var tx = new TransactionScope())
            {
                using (var sql = new SqlConnection(ConnectionString))
                {
                    sql.Execute(
                        "dbo.BulkInsertOrUpdateBulkTable",
                        new { UpdatedTable = dt },
                        commandType: CommandType.StoredProcedure);
                }

                tx.Complete();
            }

            stopwatch.Stop();

            this.textBox1.AppendText($"{stopwatch.ElapsedMilliseconds}\r\n");
        }
    }
}