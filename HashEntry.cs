using System;
using Serilog;

namespace HashDataLibrary
{
    public class HashMain
    { 
        // Used to set up the parameters required for the procedures
        // and then run each of them
        ILogger _logger;
        Source _source;

        public HashMain(ILogger logger, Source source)
        {
            // need logger and context source object
            _logger = logger;
            _source = source;
        }

    public void HashData()
    {
        HashBuilder hb = new HashBuilder(_logger, _source);
        if (_source.has_study_tables)
        {
            _logger.Information("Create study hashes\n");
            hb.CreateStudyHashes();
            _logger.Information("Study hashes created\n");

            _logger.Information("Create study composite hashes\n");
            hb.CreateStudyCompositeHashes();
            _logger.Information("Study composite hashes created\n");
        }

        _logger.Information("Create data object hashes\n");
        hb.CreateDataObjectHashes();
        _logger.Information("Data object hashes created\n");

        _logger.Information("Create data object composite hashes\n");
        hb.CreateObjectCompositeHashes();
        _logger.Information("Data object composite hashes created\n");

    }
    }
}
