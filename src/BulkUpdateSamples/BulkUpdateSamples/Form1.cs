using System;
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
        }

        private void button2_Click(object sender, EventArgs e)
        {
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
    }
}