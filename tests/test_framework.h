#pragma once

#include <cmath>
#include <iostream>
#include <sstream>
#include <string>

namespace testfw {

struct TestContext {
    int assertions = 0;
    int failures = 0;
};

inline void ReportFailure(
    TestContext& ctx,
    const std::string& expr,
    const std::string& detail,
    const char* file,
    int line) {
    ++ctx.failures;
    std::cerr << file << ":" << line
              << " Assertion failed: " << expr;
    if (!detail.empty()) {
        std::cerr << " (" << detail << ")";
    }
    std::cerr << "\n";
}

inline void ExpectTrue(
    TestContext& ctx,
    bool cond,
    const std::string& expr,
    const char* file,
    int line) {
    ++ctx.assertions;
    if (!cond) {
        ReportFailure(ctx, expr, "", file, line);
    }
}

template <typename A, typename B>
inline void ExpectEq(
    TestContext& ctx,
    const A& actual,
    const B& expected,
    const std::string& expr,
    const char* file,
    int line) {
    ++ctx.assertions;
    if (!(actual == expected)) {
        std::ostringstream oss;
        oss << "actual=" << actual << ", expected=" << expected;
        ReportFailure(ctx, expr, oss.str(), file, line);
    }
}

inline void ExpectNear(
    TestContext& ctx,
    double actual,
    double expected,
    double tol,
    const std::string& expr,
    const char* file,
    int line) {
    ++ctx.assertions;
    if (std::fabs(actual - expected) > tol) {
        std::ostringstream oss;
        oss << "actual=" << actual
            << ", expected=" << expected
            << ", tol=" << tol;
        ReportFailure(ctx, expr, oss.str(), file, line);
    }
}

} // namespace testfw

#define EXPECT_TRUE(ctx, cond) \
    ::testfw::ExpectTrue((ctx), (cond), #cond, __FILE__, __LINE__)

#define EXPECT_EQ(ctx, actual, expected) \
    ::testfw::ExpectEq((ctx), (actual), (expected), #actual " == " #expected, __FILE__, __LINE__)

#define EXPECT_NEAR(ctx, actual, expected, tol) \
    ::testfw::ExpectNear((ctx), (actual), (expected), (tol), #actual " ~= " #expected, __FILE__, __LINE__)
