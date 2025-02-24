#pragma once

#include "common/string_view.hpp"
#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {

class MppParserExtension : public ParserExtension {
public:
	MppParserExtension() {
		parse_function = Parse;
		plan_function = Plan;
	}

private:
	static ParserExtensionParseResult Parse(ParserExtensionInfo *info, const string &query);
	static ParserExtensionPlanResult Plan(ParserExtensionInfo *info, ClientContext &context,
	                                      unique_ptr<ParserExtensionParseData> parse_data);
	static unique_ptr<ParserExtensionParseData> ParseInternal(string query);
	static void Match(const string &expect, string *query);
	static bool TryMatch(const string &expect, string *query);
	static void CheckPartitionColumn(string &partition_column);
	static string NextIdentifier(string *query);
};

} // namespace duckdb