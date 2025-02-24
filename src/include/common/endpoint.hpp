#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {
struct Endpoint {
	string ip;
	uint16_t port;

	Endpoint() : ip(), port(0) {
	}

	explicit Endpoint(string ip, uint16_t port) : ip(std::move(ip)), port(port) {
	}

	bool operator==(const Endpoint &rhs) const noexcept {
		return ip == rhs.ip && port == rhs.port;
	}

	bool IsValid() const {
		return port > 0;
	}

	string ToString() const;

	static Endpoint Parse(const string &ip_port);
};
} // namespace duckdb
