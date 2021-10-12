using Serilog;

namespace HashDataLibrary
{
    public class HashBuilder
    {
        private Source _source;
        private string _connString;
        private string _schema;
        private ILogger _logger;

        public HashBuilder(ILogger logger, Source source)
        {
            _source = source;
            _connString = source.db_conn;
            _schema = _source.source_type == "test" ? "expected" : "sd";
            _logger = logger;
        }

        public void CreateStudyHashes()
        {
            StudyHashCreators hashcreator = new StudyHashCreators(_logger, _connString, _schema);
            hashcreator.create_study_record_hashes();
            hashcreator.create_study_identifier_hashes();
            hashcreator.create_study_title_hashes();

            // these are database dependent
            if (_source.has_study_topics) hashcreator.create_study_topic_hashes();
            if (_source.has_study_features) hashcreator.create_study_feature_hashes();
            if (_source.has_study_contributors) hashcreator.create_study_contributor_hashes();
            if (_source.has_study_references) hashcreator.create_study_reference_hashes();
            if (_source.has_study_relationships) hashcreator.create_study_relationship_hashes();
            if (_source.has_study_links) hashcreator.create_study_link_hashes();
            if (_source.has_study_ipd_available) hashcreator.create_ipd_available_hashes();
        }

        public void CreateStudyCompositeHashes()
        {
            StudyCompositeHashCreators hashcreator = new StudyCompositeHashCreators(_logger, _connString, _schema);
            hashcreator.recreate_table();   // ensure hash table is (re)created
            hashcreator.create_composite_study_hashes(11, "identifiers", "study_identifiers");
            hashcreator.create_composite_study_hashes(12, "titles", "study_titles");

            // these are database dependent
            if (_source.has_study_features) hashcreator.create_composite_study_hashes(13, "features", "study_features");
            if (_source.has_study_topics) hashcreator.create_composite_study_hashes(14, "topics", "study_topics");
            if (_source.has_study_contributors) hashcreator.create_composite_study_hashes(15, "contributors", "study_contributors");
            if (_source.has_study_relationships) hashcreator.create_composite_study_hashes(16, "relationships", "study_relationships");
            if (_source.has_study_references) hashcreator.create_composite_study_hashes(17, "references", "study_references");
            if (_source.has_study_links) hashcreator.create_composite_study_hashes(18, "study links", "study_links");
            if (_source.has_study_ipd_available) hashcreator.create_composite_study_hashes(19, "ipd available", "study_ipd_available");

            hashcreator.create_full_study_hashes();
        }


        public void CreateDataObjectHashes()
        {
            ObjectHashCreators hashcreator = new ObjectHashCreators(_logger, _connString, _schema);
            hashcreator.create_object_record_hashes();
            hashcreator.create_object_instance_hashes();
            hashcreator.create_object_title_hashes();

            // these are database dependent		
            if (_source.has_object_datasets) hashcreator.create_recordset_properties_hashes();
            if (_source.has_object_dates) hashcreator.create_object_date_hashes();
            if (_source.has_object_relationships) hashcreator.create_object_relationship_hashes();
            if (_source.has_object_rights) hashcreator.create_object_right_hashes();
            if (_source.has_object_pubmed_set)
            {
                hashcreator.create_object_contributor_hashes();
                hashcreator.create_object_topic_hashes();
                hashcreator.create_object_comment_hashes();
                hashcreator.create_object_description_hashes();
                hashcreator.create_object_identifier_hashes();
                hashcreator.create_object_db_link_hashes();
                hashcreator.create_object_publication_type_hashes();
            }
        }


        public void CreateObjectCompositeHashes()
        {
            ObjectCompositeHashCreators hashcreator = new ObjectCompositeHashCreators(_logger, _connString, _schema);
            hashcreator.recreate_table();   // ensure hash table is (re)created
            hashcreator.create_composite_object_hashes(51, "instances", "object_instances");
            hashcreator.create_composite_object_hashes(52, "titles", "object_titles");

            // these are database dependent		
            if (_source.has_object_datasets) hashcreator.create_composite_object_hashes(50, "datasets", "object_datasets");
            if (_source.has_object_dates) hashcreator.create_composite_object_hashes(53, "dates", "object_dates");
            if (_source.has_object_relationships) hashcreator.create_composite_object_hashes(56, "relationships", "object_relationships");
            if (_source.has_object_rights) hashcreator.create_composite_object_hashes(59, "rights", "object_rights");
            if (_source.has_object_pubmed_set)
            {
                hashcreator.create_composite_object_hashes(54, "topics", "object_topics");
                hashcreator.create_composite_object_hashes(55, "contributors", "object_contributors");
                hashcreator.create_composite_object_hashes(57, "descriptions", "object_descriptions");
                hashcreator.create_composite_object_hashes(60, "links", "object_db_links");
                hashcreator.create_composite_object_hashes(61, "comments", "object_comments");
                hashcreator.create_composite_object_hashes(62, "public types", "object_publication_types");
                hashcreator.create_composite_object_hashes(63, "identifiers", "object_identifiers");
            }

            hashcreator.create_full_data_object_hashes();
        }

    }
}

