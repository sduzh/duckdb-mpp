#pragma once

#include <string>
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

namespace duckdb {

class DataChunk;

class StringWriteStream final : public WriteStream {
public:
	explicit StringWriteStream() : string_() {
	}

	~StringWriteStream() override = default;

	//! Write data to the stream.
	void WriteData(const_data_ptr_t buffer, idx_t write_size) override {
		string_.append(reinterpret_cast<const char *>(buffer), write_size);
	}

	void Rewind() {
		string_.clear();
	}

	auto GetData() const -> const std::string & {
		return string_;
	}

private:
	std::string string_;
};

class StringReadStream final : public ReadStream {
public:
	explicit StringReadStream(std::string str) : string_(std::move(str)), position_(0) {
	}

	void ReadData(data_ptr_t buffer, idx_t read_size) override {
		if (position_ + read_size > string_.size()) {
			throw SerializationException("Failed to deserialize: not enough data in buffer to fulfill read request");
		}
		memcpy(buffer, string_.data() + position_, read_size);
		position_ += read_size;
	}

private:
	std::string string_;
	idx_t position_;
};

template <typename T>
auto SerializeToString(const T &data) {
	auto stream = StringWriteStream {};
	BinarySerializer::Serialize(data, stream);
	return stream.GetData();
}

template <typename T>
auto DeserializeFromString(const std::string &str) {
	auto stream = StringReadStream {str};
	return BinaryDeserializer::Deserialize<T>(stream);
}

template <>
inline auto DeserializeFromString<LogicalType>(const std::string &str) {
	auto stream = StringReadStream {str};
	auto deserializer = BinaryDeserializer {stream};
	deserializer.Begin();
	auto t = LogicalType::Deserialize(deserializer);
	deserializer.End();
	return t;
}

template <>
inline auto DeserializeFromString<DataChunk>(const std::string &str) {
	auto stream = StringReadStream {str};
	auto deserializer = BinaryDeserializer {stream};
	deserializer.Begin();
	auto chunk = std::make_unique<DataChunk>();
	chunk->Deserialize(deserializer);
	deserializer.End();
	return chunk;
}

} // namespace duckdb