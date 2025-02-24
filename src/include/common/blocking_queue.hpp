#include <condition_variable>
#include <optional>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/queue.hpp"

namespace duckdb {

template <typename T>
class BlockingQueue {
public:
	explicit BlockingQueue(size_t capacity) : cap_ {capacity} {
	}

	void Push(auto &&...args);

	auto Pop() -> std::optional<T>;

	auto Pop(auto &&duration) -> std::optional<T>;

	void Close(bool clear = false);

	bool IsClosed() const {
		auto l = std::lock_guard {mtx_};
		return closed_;
	}

	auto GetSize() const {
		auto l = std::lock_guard {mtx_};
		return queue_.size();
	};

private:
	const size_t cap_;
	mutable mutex mtx_ {};
	bool closed_ {false};
	queue<T> queue_ {};
	std::condition_variable read_signal_ {};
	std::condition_variable write_signal_ {};
};

template <typename T>
inline void BlockingQueue<T>::Push(auto &&...args) {
	auto l = std::unique_lock {mtx_};
	write_signal_.wait(l, [this]() { return closed_ || queue_.size() < cap_; });
	if (closed_) {
		throw std::logic_error("cannot put to an already closed blocking queue");
	}
	queue_.emplace(std::forward<decltype(args)>(args)...);
	l.unlock();
	read_signal_.notify_one();
}

template <typename T>
inline auto BlockingQueue<T>::Pop() -> std::optional<T> {
	auto l = std::unique_lock {mtx_};
	read_signal_.wait(l, [this]() { return closed_ || !queue_.empty(); });
	if (queue_.empty()) {
		return {};
	}
	T ret(std::move(queue_.front()));
	queue_.pop();
	l.unlock();
	write_signal_.notify_one();
	return {std::move(ret)};
}

template <typename T>
inline auto BlockingQueue<T>::Pop(auto &&duration) -> std::optional<T> {
	auto l = std::unique_lock {mtx_};
	read_signal_.wait_for(l, std::forward<decltype(duration)>(duration),
	                      [this]() { return closed_ || !queue_.empty(); });
	if (queue_.empty()) {
		return {};
	}
	T ret(std::move(queue_.front()));
	queue_.pop();
	l.unlock();
	write_signal_.notify_one();
	return {std::move(ret)};
}

template <typename T>
inline void BlockingQueue<T>::Close(bool clear) {
	auto l = std::unique_lock {mtx_};
	if (!closed_) {
		closed_ = true;
		if (clear) {
			auto tmp = std::queue<T> {};
			std::swap(tmp, queue_);
		}
		l.unlock();
		read_signal_.notify_all();
		write_signal_.notify_all();
	}
}

} // namespace duckdb