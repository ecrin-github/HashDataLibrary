using Dapper;
using Npgsql;
using Serilog;

namespace HashDataLibrary
{

    public class ObjectHashCreators
    {
        string db_conn;
        HashHelper h;
        private readonly ILogger _logger;
        string _schema;

        public ObjectHashCreators(ILogger logger, string _db_conn, string schema)
        {
            _logger = logger;
            db_conn = _db_conn;
            h = new HashHelper(_logger, _db_conn, schema);
            _schema = schema;
        }


        public void create_object_record_hashes()
        {
            string sql_string = @"Update " + _schema + @".data_objects
              set record_hash = md5(json_build_array(display_title, version, doi, doi_status_id, publication_year,
              object_class_id, object_type_id, managing_org_id, managing_org, managing_org_ror_id, lang_code, access_type_id,
              access_details, access_details_url, eosc_category, add_study_contribs,
              add_study_topics)::varchar)";

            h.ExecuteHashSQL(sql_string, "data_objects");
        }


        public void create_recordset_properties_hashes()
        {
            string sql_string = @"Update " + _schema + @".object_datasets
              set record_hash = md5(json_build_array(record_keys_type_id, record_keys_details,
              deident_type_id, deident_direct, deident_hipaa,
              deident_dates, deident_nonarr, deident_kanon, deident_details,
              consent_type_id, consent_noncommercial, consent_geog_restrict,
              consent_research_type, consent_genetic_only, consent_no_methods, consent_details)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_datasets");
        }


        public void create_object_date_hashes()
        {
            string sql_string = @"Update " + _schema + @".object_dates
              set record_hash = md5(json_build_array(date_type_id, date_is_range, date_as_string, 
              start_year, start_month, start_day, end_year, end_month, end_day,
              details)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_dates");
        }


        public void create_object_instance_hashes()
        {
            string sql_string = @"Update " + _schema + @".object_instances
              set record_hash = md5(json_build_array(instance_type_id, repository_org_id, 
              repository_org, url, url_accessible,  
              resource_type_id, resource_size, resource_size_units, resource_comments)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_instances");
        }


        public void create_object_title_hashes()
        {
            string sql_string = @"Update " + _schema + @".object_titles
              set record_hash = md5(json_build_array(title_text, title_type_id, lang_code, 
              lang_usage_id, is_default, comments)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_titles");

        }


        public void create_object_contributor_hashes()
        {
            string sql_string = @"Update " + _schema + @".object_contributors
              set record_hash = md5(json_build_array(contrib_type_id, is_individual, 
              person_id, person_given_name, person_family_name, person_full_name,
              orcid_id, person_affiliation, 
              organisation_id, organisation_name, organisation_ror_id)::varchar)";
            h.ExecuteHashSQL(sql_string, "object_contributors");
        }


        public void create_object_topic_hashes()
        {
            string sql_string = @"Update " + _schema + @".object_topics
              set record_hash = md5(json_build_array(topic_type_id, 
              mesh_coded, mesh_code, mesh_value, 
              original_ct_id, original_ct_code,
              original_value)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_topics");
        }


        public void create_object_comment_hashes()
        {
            string sql_string = @"Update " + _schema + @".object_comments
              set record_hash = md5(json_build_array(ref_type, ref_source, pmid, pmid_version,
              notes)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_comments");
        }


        public void create_object_description_hashes()
        {
            string sql_string = @"Update " + _schema + @".object_descriptions
              set record_hash = md5(json_build_array(description_type_id, label, description_text, 
              lang_code)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_descriptions");
        }


        public void create_object_identifier_hashes()
        {
            string sql_string = @"Update " + _schema + @".object_identifiers
              set record_hash = md5(json_build_array(identifier_value, identifier_type_id, 
                                identifier_org_id, identifier_org, identifier_org_ror_id, identifier_date)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_identifiers");
        }


        public void create_object_db_link_hashes()
        {
            string sql_string = @"Update " + _schema + @".object_db_links
              set record_hash = md5(json_build_array(db_sequence, db_name, 
              id_in_db)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_db_links");
        }


        public void create_object_publication_type_hashes()
        {
            string sql_string = @"Update " + _schema + @".object_publication_types
              set record_hash = md5(json_build_array(type_name)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_publication_types");
        }


        public void create_object_relationship_hashes()
        {
            string sql_string = @"Update " + _schema + @".object_relationships
              set record_hash = md5(json_build_array(relationship_type_id,
                                target_sd_oid)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_relationships");
        }


        public void create_object_right_hashes()
        {
            string sql_string = @"Update " + _schema + @".object_rights
              set record_hash = md5(json_build_array(rights_name, 
                                rights_uri, comments)::varchar)";

            h.ExecuteHashSQL(sql_string, "object_rights");
        }

    }


    public class ObjectCompositeHashCreators
    {
        string db_conn;
        HashHelper h;
        private readonly ILogger _logger;
        string _schema;

        public ObjectCompositeHashCreators(ILogger logger, string _db_conn, string schema)
        {
            _logger = logger;
            db_conn = _db_conn;
            h = new HashHelper(_logger, db_conn, schema);
            _schema = schema;
        }


        public void recreate_table()
        {
            string sql_string = @"DROP TABLE IF EXISTS " + _schema + @".object_hashes;";
            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }

            sql_string = @"CREATE TABLE " + _schema + @".object_hashes(
                id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
              , sd_oid                 VARCHAR         NOT NULL
              , hash_type_id           INT             NULL
              , hash_type              VARCHAR         NULL
              , composite_hash         CHAR(32)        NULL
            );
            CREATE INDEX object_hashes_sd_oid ON " + _schema + @".object_hashes(sd_oid);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_composite_object_hashes(int hash_type_id, string hash_type, string table_name)
        {
            string top_sql_string = @"Insert into " + _schema + @".object_hashes 
                    (sd_oid, hash_type_id, hash_type, composite_hash)
                    select t.sd_oid, " + hash_type_id.ToString() + ", '" + hash_type + @"',  
                    md5(to_json(array_agg(t.record_hash ORDER BY t.record_hash))::varchar)
                    from " + _schema + @"." + table_name;

            h.CreateCompositeOjectHashes(top_sql_string, hash_type_id, hash_type, table_name);
        }


        public void create_full_data_object_hashes()
        {
            // needs to roll up, for any particular data object
            // all of the composite hashes plus any hash for a dataset property record,
            // plus the data object record itself
            string sql_string = @"update " + _schema + @".data_objects d
            set object_full_hash = b.roll_up from
            (select sd_oid, 
             md5(to_json(array_agg(hash ORDER BY hash))::varchar) as roll_up
              from 
                (select sd_oid, composite_hash as hash   
                from " + _schema + @".object_hashes
                union
                select sd_oid, record_hash as hash
                from " + _schema + @".data_objects) h
             group by sd_oid) b
             where d.sd_oid = b.sd_oid";

            h.CreateFullDataObjectHashes(sql_string);
        }
    }
}
