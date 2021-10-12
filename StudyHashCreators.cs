using Dapper;
using Npgsql;
using System;
using Serilog;

namespace HashDataLibrary
{
   
    public class StudyHashCreators
     {
        string db_conn;
        HashHelper h;
        private readonly ILogger _logger;
        string _schema;

        public StudyHashCreators(ILogger logger, string _db_conn, string schema)
        {
            _logger = logger;
            db_conn = _db_conn;
            h = new HashHelper(_logger, db_conn, schema);
            _schema = schema;
        }

        public void create_study_record_hashes()
        {
            string sql_string = @"Update " + _schema + @".studies
              set record_hash = md5(json_build_array(display_title, brief_description, 
              data_sharing_statement, study_start_year, study_start_month, study_type_id,
              study_status_id, study_enrolment, study_gender_elig_id, min_age,
              min_age_units_id, max_age, max_age_units_id)::varchar)";

            h.ExecuteHashSQL(sql_string, "studies");
        }


        public void create_study_identifier_hashes()
        {
            string sql_string = @"Update " + _schema + @".study_identifiers
              set record_hash = md5(json_build_array(identifier_value, identifier_type_id, identifier_org_id,
              identifier_org, identifier_org_ror_id, identifier_date, identifier_link)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_identifiers");
        }


        public void create_study_title_hashes()
        {
            string sql_string = @"Update " + _schema + @".study_titles
              set record_hash = md5(json_build_array(title_text, title_type_id, lang_code,
              lang_usage_id, is_default, comments)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_titles");
        }


        public void create_study_contributor_hashes()
        {
            string sql_string = @"Update " + _schema + @".study_contributors
              set record_hash = md5(json_build_array(contrib_type_id, is_individual, 
              person_id, person_given_name, person_family_name, person_full_name,
              orcid_id, person_affiliation, 
              organisation_id, organisation_name, organisation_ror_id)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_contributors");
        }


        public void create_study_topic_hashes()
        {
            string sql_string = @"Update " + _schema + @".study_topics
            set record_hash = md5(json_build_array(topic_type_id,
              mesh_coded, mesh_code, mesh_value, 
              original_ct_id, original_ct_code,
              original_value)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_topics");
        }


        public void create_study_feature_hashes()
        {
            string sql_string = @"Update " + _schema + @".study_features
              set record_hash = md5(json_build_array(feature_type_id, feature_value_id)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_features");
        }


        public void create_study_reference_hashes()
        {
            string sql_string = @"Update " + _schema + @".study_references
              set record_hash = md5(json_build_array(pmid, citation, doi, comments)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_references");
        }


        public void create_study_relationship_hashes()
        {
            string sql_string = @"Update " + _schema + @".study_relationships
              set record_hash = md5(json_build_array(relationship_type_id, target_sd_sid)::varchar)";
           
            h.ExecuteHashSQL(sql_string, "study_relationships");
        }


        public void create_study_link_hashes()
        {
            string sql_string = @"Update " + _schema + @".study_links
              set record_hash = md5(json_build_array(link_label, link_url)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_links");
        }


        public void create_ipd_available_hashes()
        {
            string sql_string = @"Update " + _schema + @".study_ipd_available
              set record_hash = md5(json_build_array(ipd_id, ipd_type, ipd_url, ipd_comment)::varchar)";

            h.ExecuteHashSQL(sql_string, "study_ipd_available");
        }

    }
    
    
    public class StudyCompositeHashCreators
    {
        string db_conn;
        HashHelper h;
        private readonly ILogger _logger;
        string _schema;

        public StudyCompositeHashCreators(ILogger logger, string _db_conn, string schema)
        {
            _logger = logger; 
            db_conn = _db_conn;
            h = new HashHelper(_logger, db_conn, schema);
            _schema = schema;
        }


        public void recreate_table()
        {
            string sql_string = @"DROP TABLE IF EXISTS " + _schema + @".study_hashes;";
            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }

            sql_string = @"CREATE TABLE " + _schema + @".study_hashes(
                id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
              , sd_sid                 VARCHAR         NOT NULL
              , hash_type_id           INT             NULL
              , hash_type              VARCHAR         NULL
              , composite_hash         CHAR(32)        NULL
            );
            CREATE INDEX study_hashes_sd_sid ON " + _schema + @".study_hashes(sd_sid);";

            using (var conn = new NpgsqlConnection(db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void create_composite_study_hashes(int hash_type_id, string hash_type, string table_name)
        {
            string top_sql_string = @"Insert into " + _schema + @".study_hashes 
                    (sd_sid, hash_type_id, hash_type, composite_hash)
                    select t.sd_sid, " + hash_type_id.ToString() + ", '" + hash_type + @"',  
                    md5(to_json(array_agg(t.record_hash ORDER BY t.record_hash))::varchar)
                    from " + _schema + @"." + table_name;

            h.CreateCompositeStudyHashes(top_sql_string, hash_type_id, hash_type, table_name);

        }

        public void create_full_study_hashes()
        {
            string sql_string = @"update " + _schema + @".studies s
            set study_full_hash = b.rollup
            from (select sd_sid, md5(to_json(array_agg(hash ORDER BY hash))::varchar) as rollup
                  from 
                     (select sd_sid, composite_hash as hash 
                      from " + _schema + @".study_hashes
                      union
                      select sd_sid, record_hash as hash
                      from " + _schema + @".studies) h
                  group by sd_sid) b
            where 
            s.sd_sid = b.sd_sid";

            h.CreateFullStudyHashes(sql_string);
        }
    }

}
