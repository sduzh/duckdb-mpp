#include <storage/mpp_nodes.hpp>
#include "parser/mpp_parser_extension.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "function/create_distributed_table.hpp"
#include "parser/mpp_parse_data.hpp"
#include "parser/partition_method.hpp"
#include "storage/mpp_catalog.hpp"

namespace duckdb {
ParserExtensionParseResult MppParserExtension::Parse(ParserExtensionInfo *info, const string &query) {
	try {
		auto data = ParseInternal(query);
		return ParserExtensionParseResult(std::move(data));
	} catch (...) {
		return {};
	}
}

ParserExtensionPlanResult MppParserExtension::Plan(ParserExtensionInfo *info, ClientContext &context,
                                                   unique_ptr<ParserExtensionParseData> parse_data) {
	auto data = unique_ptr_cast<ParserExtensionParseData, MppCreateDistributedTableData>(std::move(parse_data));
	auto &catalog = Catalog::GetCatalog(context, data->base->catalog);
	if (catalog.GetCatalogType() != "mpp") {
		throw BinderException("Cannot create distributed table in database '%s'", catalog.GetName());
	}
	if (data->base->catalog.empty()) {
		data->base->catalog = catalog.GetName();
	}
	if (data->base->schema.empty()) {
		data->base->schema = catalog.GetDefaultSchema();
	}
	auto result = ParserExtensionPlanResult {};
	result.function = CreateDistributedTableFunction::GetFunction();
	result.parameters = {data->base->ToString(), data->partition_column, data->buckets};
	result.modified_databases[catalog.GetName()] = {.catalog_oid = catalog.GetOid(),
	                                                .catalog_version = catalog.GetCatalogVersion(context)};
	return result;
}

unique_ptr<ParserExtensionParseData> MppParserExtension::ParseInternal(std::string query) {
	auto idx = StringUtil::Find(StringUtil::Upper(query), "PARTITION");
	if (!idx.IsValid()) {
		throw SyntaxException("Not a distributed table creation statement");
	}
	auto create_table_query = query.substr(0, idx.GetIndex());
	auto parser = Parser();
	parser.ParseQuery(create_table_query);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT) {
		throw SyntaxException("Not a distributed table creation statement");
	}
	auto &create_statement = parser.statements[0]->Cast<CreateStatement>();
	if (create_statement.info->type != CatalogType::TABLE_ENTRY) {
		throw SyntaxException("Not a distributed table creation statement");
	}
	auto create_table_info = unique_ptr_cast<CreateInfo, CreateTableInfo>(std::move(create_statement.info));
	if (create_table_info->temporary || create_table_info->internal || create_table_info->query) {
		throw SyntaxException("Not a distributed table creation statement");
	}

	query = query.substr(idx.GetIndex());
	Match("PARTITION", &query);
	Match("BY", &query);
	Match("(", &query);
	idx = StringUtil::Find(query, ")");
	if (!idx.IsValid()) {
		throw SyntaxException("Not a distributed table creation statement");
	}

	auto partition_column = query.substr(0, idx.GetIndex());
	CheckPartitionColumn(partition_column);

	query = query.substr(idx.GetIndex());
	Match(")", &query);
	Match("WITH", &query);
	Match("BUCKETS", &query);
	StringUtil::Trim(query);
	if (query.empty()) {
		throw SyntaxException("Not a distributed table creation statement");
	}
	char *endptr;
	auto buckets = std::strtol(query.data(), &endptr, 10);
	if (*endptr != '\0' && *endptr != ';') {
		throw SyntaxException("Not a distributed table creation statement");
	}
	if (buckets > INT32_MAX) {
		throw SyntaxException("Not a distributed table creation statement");
	}
	auto result = make_uniq<MppCreateDistributedTableData>();
	result->base = std::move(create_table_info);
	result->partition_column = partition_column;
	result->buckets = (int32_t)buckets;
	return result;
}

void MppParserExtension::Match(const string &expect, string *query) {
	if (!TryMatch(expect, query)) {
		throw SyntaxException("Expect token %s but got %s", string(expect), string(*query));
	}
}

bool MppParserExtension::TryMatch(const string &expect, string *query) {
	StringUtil::LTrim(*query);
	if (!StringUtil::StartsWith(StringUtil::Upper(*query), StringUtil::Upper(expect))) {
		return false;
	}
	*query = query->substr(expect.size());
	return true;
}

void MppParserExtension::CheckPartitionColumn(string &partition_column) {
	StringUtil::Trim(partition_column);
	if (partition_column.empty()) {
		throw SyntaxException("No partition column");
	}
	if (partition_column.front() == '"') {
		if (partition_column.size() <= 2 || partition_column.back() != '"') {
			throw SyntaxException("Unmatched quote or empty quoted string");
		}
		partition_column = partition_column.substr(1, partition_column.size() - 2);
	}
}

string MppParserExtension::NextIdentifier(string *query) {
	StringUtil::Trim(*query);
	auto result = string {};
	for (auto c : *query) {
		if (c == '_' || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')) {
			result.push_back(c);
		} else {
			break;
		}
	}
	query->erase(0, result.size());
	return result;
}

} // namespace duckdb
