using System.Collections.Generic;
using Parquet.Schema;

namespace HubClient.Core.Storage
{
    /// <summary>
    /// Generator for Parquet schemas based on different input types
    /// </summary>
    public interface ISchemaGenerator
    {
        /// <summary>
        /// Generates a Parquet schema from a row dictionary
        /// </summary>
        /// <param name="row">Sample row containing data types</param>
        /// <returns>Generated Parquet schema</returns>
        ParquetSchema GenerateSchema(IDictionary<string, object> row);
    }
} 