using Dapper.Contrib.Extensions;
using Microsoft.Extensions.Configuration;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace HashDataLibrary
{
    public class Credentials
    {
        public string Host { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }

        public Credentials(string host, string user, string password)
        {
            Host = host;
            Username = user;
            Password = password;
        }

        public string GetConnectionString(string database_name, int harvest_type_id)
        {
            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
            builder.Host = Host;
            builder.Username = Username;
            builder.Password = Password;
            builder.Database = (harvest_type_id == 3) ? "test" : database_name;
            return builder.ConnectionString;
        }
    }


    [Table("sf.source_parameters")]
    public class Source
    {
        public int id { get; }
        public string source_type { get; }
        public string database_name { get; }
        public string db_conn { get; set; }
        public int default_harvest_type_id { get; }
        public bool requires_file_name { get; }
        public bool uses_who_harvest { get; }
        public int harvest_chunk { get; }
        public string local_folder { get; }
        public bool? local_files_grouped { get; }
        public int? grouping_range_by_id { get; }
        public string local_file_prefix { get; }
        public bool has_study_tables { get; }
        public bool has_study_topics { get; }
        public bool has_study_features { get; }
        public bool has_study_contributors { get; }
        public bool has_study_references { get; }
        public bool has_study_relationships { get; }
        public bool has_study_links { get; }
        public bool has_object_datasets { get; }
        public bool has_study_ipd_available { get; }
        public bool has_object_dates { get; }
        public bool has_object_rights { get; }
        public bool has_object_relationships { get; }
        public bool has_object_pubmed_set { get; }

        public Source(int _id, string _database_name, string _source_type, string _db_conn,
                      bool _has_study_tables, bool _has_study_topics, bool _has_study_features,
                      bool _has_study_contributors, bool _has_study_references, bool _has_study_relationships,
                      bool _has_study_links, bool _has_study_ipd_available, bool _has_object_datasets,
                      bool _has_object_dates, bool _has_object_rights, bool _has_object_relationships,
                      bool _has_object_pubmed_set)
        {
            id = _id;
            source_type = _source_type;
            database_name = _database_name;
            db_conn = _db_conn;
            has_study_tables = _has_study_tables;
            has_study_topics = _has_study_topics;
            has_study_features = _has_study_features;
            has_study_contributors = _has_study_contributors;
            has_study_references = _has_study_references;
            has_study_relationships = _has_study_relationships;
            has_study_links = _has_study_links;
            has_study_ipd_available = _has_study_ipd_available;
            has_object_datasets = _has_object_datasets;
            has_object_dates = _has_object_dates;
            has_object_rights = _has_object_rights;
            has_object_relationships = _has_object_relationships;
            has_object_pubmed_set = _has_object_pubmed_set;
        }
    }
}

