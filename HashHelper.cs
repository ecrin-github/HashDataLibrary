using Dapper;
using Npgsql;
using Serilog;
using System;
using System.Collections.Generic;
using System.Text;

namespace HashDataLibrary
{
    public class HashHelper
    {
        string db_conn;
        private readonly ILogger _logger;
        string _schema;

        public HashHelper(ILogger logger, string _db_conn, string schema)
        {
            db_conn = _db_conn;
            _logger = logger;
            _schema = schema;
        }

        public int GetRecordCount(string table_name)
        {
            int res = 0;
            string sql_string = @"select count(*) from " + _schema + @"." + table_name;
            using (var conn = new NpgsqlConnection(db_conn))
            {
                res = conn.ExecuteScalar<int>(sql_string);
            }
            return res;
        }

        public int GetHashRecordCount(string table_name, int hash_type_id)
        {
            int res = 0;
            string sql_string = @"select count(*) from " + _schema + @"." + table_name +
                " where hash_type_id = " + hash_type_id.ToString();
            using (var conn = new NpgsqlConnection(db_conn))
            {
                res = conn.ExecuteScalar<int>(sql_string);
            }
            return res;
        }


        public void ExecuteSQL(string sql_string)
        {
            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void ExecuteHashSQL(string sql_string, string table_name)
        {
            try
            {
                int rec_count = GetRecordCount(table_name);
                if (rec_count > 0)
                {
                    int rec_batch = 500000;
                    // int rec_batch = 10000;  // for testing 
                    if (rec_count > rec_batch)
                    {
                        for (int r = 1; r <= rec_count; r += rec_batch)
                        {
                            string batch_sql_string = sql_string + " where id >= " + r.ToString() + " and id < " + (r + rec_batch).ToString();
                            ExecuteSQL(batch_sql_string);
                            string feedback = "Creating " + table_name + " hash codes, " + r.ToString() + " to ";
                            feedback += (r + rec_batch < rec_count) ? (r + rec_batch - 1).ToString() : rec_count.ToString();
                            _logger.Information(feedback);
                        }
                    }
                    else
                    {
                        ExecuteSQL(sql_string);
                        _logger.Information("Creating " + table_name + " hash codes - as a single batch");
                    }
                }
            }
            catch (Exception e)
            {
                string res = e.Message;
                _logger.Error("In hash creation (" + table_name + "): " + res);
            }
        }


        public void CreateCompositeOjectHashes(string top_sql_string, int hash_type_id, string hash_type, string table_name)
        {
            try
            {
                int rec_count = GetRecordCount(table_name);
                if (rec_count > 0)
                {
                    int objectcount = GetRecordCount("data_objects");
                    int num_recs_per_entity = rec_count / objectcount;
                    bool use_batch = false;
                    int rec_batch = 0;

                    if (rec_count > 50000)
                    {
                        use_batch = true;
                        rec_batch = 50000;
                        if (num_recs_per_entity > 2)
                        {
                            rec_batch = 50000 / (2 * num_recs_per_entity);
                        }
                    }

                    if (use_batch)
                    {
                        for (int r = 1; r <= rec_count; r += rec_batch)
                        {
                            string where_sql_string = " where d.id >= " + r.ToString() + " and d.id < " + (r + rec_batch).ToString();

                            string batch_sql_string = top_sql_string + @" t 
                                 inner join " + _schema + @".data_objects d 
                                 on d.sd_oid = t.sd_oid 
                                 " + where_sql_string + " group by t.sd_oid;";
                            ExecuteSQL(batch_sql_string);
                            string feedback = "Creating composite object hash codes (" + hash_type + "), " + r.ToString() + " to ";
                            feedback += (r + rec_batch < rec_count) ? (r + rec_batch - 1).ToString() : rec_count.ToString();
                            _logger.Information(feedback);
                        }
                    }
                    else
                    {
                        string sql_string = top_sql_string + @" t group by t.sd_oid;";
                        ExecuteSQL(sql_string);
                        _logger.Information("Creating composite object hash codes (" + hash_type + ") as a single batch");
                    }
                }

            }
            catch (Exception e)
            {
                string res = e.Message;
                _logger.Error("In object composite hash creation (" + hash_type + "): " + res);
            }
        }

        public void CreateFullDataObjectHashes(string top_sql_string)
        {
            try
            {
                int rec_count = GetRecordCount("data_objects");
                int rec_batch = 50000;
                // int rec_batch = 1000;  // for testing 
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string where_sql_string = " and d.id >= " + r.ToString() + " and d.id < " + (r + rec_batch).ToString();
                        string batch_sql_string = top_sql_string + where_sql_string;
                        ExecuteSQL(batch_sql_string);
                        string feedback = "Creating full object hash codes, " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch - 1).ToString() : rec_count.ToString();
                        _logger.Information(feedback);
                    }
                }
                else
                {
                    ExecuteSQL(top_sql_string);
                    _logger.Information("Creating full object hash codes - as a single batch");
                }

            }
            catch (Exception e)
            {
                string res = e.Message;
                _logger.Error("In full hash creation for data objects: " + res);
            }
        }


        public void CreateCompositeStudyHashes(string top_sql_string, int hash_type_id, string hash_type, string table_name)
        {
            try
            {
                int rec_count = GetRecordCount(table_name);
                if (rec_count > 0)
                {
                    int study_count = GetRecordCount("studies");
                    int rec_batch = 10000;
                    //int rec_batch = 1000;  // for testing 
                    if (study_count > rec_batch)
                    {
                        for (int r = 1; r <= study_count; r += rec_batch)
                        {
                            string where_sql_string = " where s.id >= " + r.ToString() + " and s.id < " + (r + rec_batch).ToString();

                            string batch_sql_string = top_sql_string + @" t 
                                 inner join " + _schema + @".studies s 
                                 on s.sd_sid = t.sd_sid 
                                 " + where_sql_string + " group by t.sd_sid;";
                            ExecuteSQL(batch_sql_string);
                            string feedback = "Creating composite study hash codes (" + hash_type + "), " + r.ToString() + " to ";
                            feedback += (r + rec_batch < study_count) ? (r + rec_batch - 1).ToString() : study_count.ToString();
                            _logger.Information(feedback);
                        }
                    }
                    else
                    {
                        string sql_string = top_sql_string + @" t group by t.sd_sid;";
                        ExecuteSQL(sql_string);
                        _logger.Information("Creating composite study hash codes (" + hash_type + ") as a single batch");
                    }
                }
            }
            catch (Exception e)
            {
                string res = e.Message;
                _logger.Error("In study composite hash creation: " + res);
            }
        }

        public void CreateFullStudyHashes(string top_sql_string)
        {
            try
            {
                int rec_count = GetRecordCount("studies");
                int rec_batch = 50000;
                //int rec_batch = 1000;  // for testing 
                if (rec_count > rec_batch)
                {
                    for (int r = 1; r <= rec_count; r += rec_batch)
                    {
                        string where_sql_string = " and s.id >= " + r.ToString() + " and s.id < " + (r + rec_batch).ToString();
                        string batch_sql_string = top_sql_string + where_sql_string;
                        ExecuteSQL(batch_sql_string);
                        string feedback = "Creating full study hash codes, " + r.ToString() + " to ";
                        feedback += (r + rec_batch < rec_count) ? (r + rec_batch - 1).ToString() : rec_count.ToString();
                        _logger.Information(feedback);
                    }
                }
                else
                {
                    ExecuteSQL(top_sql_string);
                    _logger.Information("Creating full study hash codes - as a single batch");
                }

            }
            catch (Exception e)
            {
                string res = e.Message;
                _logger.Error("In full hash creation for studies: " + res);
            }
        }

    }

}
