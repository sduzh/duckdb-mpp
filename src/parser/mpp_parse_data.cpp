#include "parser/mpp_parse_data.hpp"

#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

static string QualifierToString(const string &catalog, const string &schema, const string &name) {
	string result;
	if (!catalog.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(catalog) + ".";
		if (!schema.empty()) {
			result += KeywordHelper::WriteOptionallyQuoted(schema) + ".";
		}
	} else if (!schema.empty() && schema != DEFAULT_SCHEMA) {
		result += KeywordHelper::WriteOptionallyQuoted(schema) + ".";
	}
	result += KeywordHelper::WriteOptionallyQuoted(name);
	return result;
}

unique_ptr<ParserExtensionParseData> MppCreateDistributedTableData::Copy() const {
	auto result = make_uniq<MppCreateDistributedTableData>();
	result->base = unique_ptr_cast<CreateInfo, CreateTableInfo>(base->Copy());
	result->partition_column = partition_column;
	result->buckets = buckets;
	return result;
}

string MppCreateDistributedTableData::ToString() const {
	string ret = "";

	ret += "CREATE";
	if (base->on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		ret += " OR REPLACE";
	}
	ret += " TABLE ";

	if (base->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		ret += " IF NOT EXISTS ";
	}
	ret += QualifierToString(base->catalog, base->schema, base->table);
	ret += TableCatalogEntry::ColumnsToSQL(base->columns, base->constraints);
	ret += " PARTITION BY ";
	ret += "(" + KeywordHelper::WriteOptionallyQuoted(partition_column) + ")";
	ret += " WITH BUCKETS " + to_string(buckets);
	ret += ";";
	return ret;
}
} // namespace duckdb