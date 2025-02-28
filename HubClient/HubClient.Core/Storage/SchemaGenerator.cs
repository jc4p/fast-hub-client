using System;
using System.Collections.Generic;
using System.Linq;
using Parquet.Data;
using Parquet.Schema;

namespace HubClient.Core.Storage
{
    /// <summary>
    /// Default implementation of the schema generator
    /// </summary>
    public class SchemaGenerator : ISchemaGenerator
    {
        /// <summary>
        /// Generates a Parquet schema from a row dictionary
        /// </summary>
        /// <param name="row">Sample row containing data types</param>
        /// <returns>Generated Parquet schema</returns>
        public ParquetSchema GenerateSchema(IDictionary<string, object> row)
        {
            if (row == null || row.Count == 0)
                throw new ArgumentException("Cannot generate schema from empty row", nameof(row));
                
            var fields = new List<Field>();
            
            foreach (var kvp in row)
            {
                var field = CreateField(kvp.Key, kvp.Value);
                if (field != null)
                {
                    fields.Add(field);
                }
            }
            
            return new ParquetSchema(fields);
        }
        
        /// <summary>
        /// Creates a field for a schema based on a key and value
        /// </summary>
        /// <param name="name">Field name</param>
        /// <param name="value">Field value</param>
        /// <returns>Data field or null if type is not supported</returns>
        private static Field CreateField(string name, object value)
        {
            return value switch
            {
                null => new DataField<string>(name),
                string => new DataField<string>(name),
                int => new DataField<int>(name),
                long => new DataField<long>(name),
                float => new DataField<float>(name),
                double => new DataField<double>(name),
                bool => new DataField<bool>(name),
                DateTime => new DataField<DateTimeOffset>(name),
                DateTimeOffset => new DataField<DateTimeOffset>(name),
                byte[] => new DataField<byte[]>(name),
                _ => new DataField<string>(name) // Convert other types to string
            };
        }
    }
} 