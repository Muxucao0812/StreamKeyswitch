#pragma once

#include "backend/cycle_backend/bram_tracker.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <utility>

class BramAccounting {
public:
    explicit BramAccounting(uint64_t budget_bytes)
        : tracker_(budget_bytes) {}

    void AcquireOnIssue(uint64_t bytes) {
        AddIssue(ToSigned(bytes));
    }

    void AcquireOnComplete(uint64_t bytes) {
        AddComplete(ToSigned(bytes));
    }

    void ReleaseOnIssue(uint64_t bytes) {
        AddIssue(-ToSigned(bytes));
    }

    void ReleaseOnComplete(uint64_t bytes) {
        AddComplete(-ToSigned(bytes));
    }

    bool CanAcquire(uint64_t bytes) const {
        const uint64_t live = Live();
        return (live + bytes) <= tracker_.Budget();
    }
    uint64_t Budget() const { return tracker_.Budget(); }
    uint64_t Remaining() const {
        const uint64_t live = Live();
        return (live < tracker_.Budget()) ? (tracker_.Budget() - live) : 0;
    }
    bool Overflowed() const {
        return tracker_.Overflowed() || (Live() > tracker_.Budget());
    }
    uint64_t Peak() const { return std::max<uint64_t>(tracker_.Peak(), Live()); }
    uint64_t Live() const {
        return ApplyUnsignedDelta(
            ApplyUnsignedDelta(tracker_.Live(), pending_issue_delta_),
            pending_complete_delta_);
    }

    std::pair<int64_t, int64_t> FlushGroupDeltas() {
        const std::pair<int64_t, int64_t> deltas{
            pending_issue_delta_,
            pending_complete_delta_
        };
        ApplyToTracker(deltas.first);
        ApplyToTracker(deltas.second);
        pending_issue_delta_ = 0;
        pending_complete_delta_ = 0;
        return deltas;
    }

private:
    static uint64_t ApplyUnsignedDelta(uint64_t value, int64_t delta) {
        if (delta >= 0) {
            return value + static_cast<uint64_t>(delta);
        }
        const uint64_t released = static_cast<uint64_t>(-delta);
        return (released >= value) ? 0 : (value - released);
    }

    static int64_t ToSigned(uint64_t bytes) {
        const uint64_t max_i64 =
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
        return (bytes > max_i64)
            ? std::numeric_limits<int64_t>::max()
            : static_cast<int64_t>(bytes);
    }

    void ApplyToTracker(int64_t delta) {
        if (delta >= 0) {
            tracker_.Acquire(static_cast<uint64_t>(delta));
            return;
        }
        tracker_.Release(static_cast<uint64_t>(-delta));
    }

    static void SaturatingAdd(int64_t* target, int64_t delta) {
        if (delta > 0) {
            const int64_t max_i64 = std::numeric_limits<int64_t>::max();
            if (*target > (max_i64 - delta)) {
                *target = max_i64;
                return;
            }
        } else if (delta < 0) {
            const int64_t min_i64 = std::numeric_limits<int64_t>::min();
            if (*target < (min_i64 - delta)) {
                *target = min_i64;
                return;
            }
        }
        *target += delta;
    }

    void AddIssue(int64_t delta) { SaturatingAdd(&pending_issue_delta_, delta); }
    void AddComplete(int64_t delta) { SaturatingAdd(&pending_complete_delta_, delta); }

    BramTracker tracker_;
    int64_t pending_issue_delta_ = 0;
    int64_t pending_complete_delta_ = 0;
};
