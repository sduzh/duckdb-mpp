#include "common/endpoint.hpp"

#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {
string Endpoint::ToString() const {
	return ip + ":" + std::to_string(port);
}

Endpoint Endpoint::Parse(const string &ip_port) {
	auto parts = StringUtil::Split(ip_port, ":");
	if (parts.size() != 2) {
		throw InvalidInputException("Invalid endpoint: %s", ip_port);
	}
	auto result = Endpoint {};
	result.ip = std::move(parts[0]);
	result.port = Cast::Operation<string_t, uint16_t>(parts[1]);
	return result;
}
} // namespace duckdb