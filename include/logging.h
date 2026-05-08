#pragma once

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>

namespace logging {

/// @brief An enum to track the type of status
enum StatusType {
  Ok,
  InternalError,
  Unavailable,
  Aborted,
};

/// @brief A status object that can be used to track the status of an operation
struct Status {
  StatusType t;
  std::optional<std::string> message;

  static Status OkStatus() { return {StatusType::Ok, {}}; }

  template <typename T> Status operator<<(T t_) {
    std::string curr = message ? message.value() : "";
    std::stringstream s;
    s << curr;
    s << t_;
    message = s.str();
    return *this;
  }
};

/// @brief A simple struct that contains the status with its value
template <class T> struct StatusVal {
  Status status;
  std::optional<T> val;
};

} // namespace logging

// -----------------------------------------------------------------------------
// Logging implementation
// -----------------------------------------------------------------------------
namespace logging {

#define RELEASE 0
#define DEBUG 1

#ifndef LOG_LEVEL
#warning "LOG_LEVEL is not defined... defaulting to DEBUG"
#define LOG_LEVEL DEBUG
#endif

#if LOG_LEVEL != RELEASE && LOG_LEVEL != DEBUG
#warning "Invalid value for LOG_LEVEL. Defaulting to DEBUG"
#define LOG_LEVEL DEBUG
#endif

// -----------------------------------------------------------------------------
// Minimal formatting helper (NO std::format)
// -----------------------------------------------------------------------------
namespace detail {

// Base case: no arguments left
inline void format_into(std::ostringstream &os, std::string_view fmt) {
  os << fmt;
}

template <typename T, typename... Rest>
inline void format_into(std::ostringstream &os, std::string_view fmt, T &&value,
                        Rest &&...rest) {
  size_t pos = fmt.find("{}");
  if (pos == std::string_view::npos) {
    // No more placeholders — append remaining args
    os << fmt << " " << std::forward<T>(value);
    (..., (os << rest));
    return;
  }

  os << fmt.substr(0, pos);
  os << std::forward<T>(value);
  format_into(os, fmt.substr(pos + 2), std::forward<Rest>(rest)...);
}

template <typename... Args>
inline std::string format(std::string_view fmt, Args &&...args) {
  std::ostringstream os;
  format_into(os, fmt, std::forward<Args>(args)...);
  return os.str();
}

} // namespace detail

// -----------------------------------------------------------------------------
// Printing helpers
// -----------------------------------------------------------------------------
inline void print_debug(std::string_view msg, const char *file, uint32_t line) {
  std::printf("[DEBUG] %.*s (%s:%u)\n", static_cast<int>(msg.size()),
              msg.data(), file, line);
  std::fflush(stdout);
}

inline void print_info(std::string_view msg) {
  std::printf("[INFO] %.*s\n", static_cast<int>(msg.size()), msg.data());
  std::fflush(stdout);
}

inline void print_fatal(std::string_view msg) {
  std::printf("[FATAL] %.*s\n", static_cast<int>(msg.size()), msg.data());
  std::fflush(stdout);
}

// -----------------------------------------------------------------------------
// Macros (API UNCHANGED)
// -----------------------------------------------------------------------------
#if LOG_LEVEL == DEBUG
#define LOGGING_DEBUG(...)                                                     \
  logging::print_debug(logging::detail::format(__VA_ARGS__), __FILE__, __LINE__)
#else
#define LOGGING_DEBUG(...)
#endif

#define LOGGING_INFO(...)                                                      \
  logging::print_info(logging::detail::format(__VA_ARGS__))

#define LOGGING_FATAL(...)                                                     \
  do {                                                                         \
    logging::print_fatal(logging::detail::format(__VA_ARGS__));                \
    std::_Exit(1);                                                             \
  } while (0)

#define LOGGING_ASSERT(check, ...)                                             \
  do {                                                                         \
    if (!(check)) [[unlikely]] {                                               \
      logging::print_fatal(logging::detail::format(__VA_ARGS__));              \
      std::_Exit(1);                                                           \
    }                                                                          \
  } while (0)

// -----------------------------------------------------------------------------
// Init helper
// -----------------------------------------------------------------------------
inline void INIT() {
#if LOG_LEVEL == DEBUG
  std::printf("logging::DEBUG is true\n");
#else
  std::printf("logging::DEBUG is false\n");
#endif
}

} // namespace logging
