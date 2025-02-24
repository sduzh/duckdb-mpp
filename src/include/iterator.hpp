#pragma once

#include <optional>

namespace duckdb {

template <typename T>
class Iterator {
public:
	virtual ~Iterator() = default;

	virtual bool Next() = 0;

	virtual T value() = 0;
};

} // namespace duckdb