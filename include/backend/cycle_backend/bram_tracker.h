#pragma once

#include <algorithm>
#include <cstdint>

class BramTracker {
public:
    explicit BramTracker(uint64_t budget_bytes)
        : budget_(budget_bytes) {}

    void Acquire(uint64_t bytes) {
        live_ += bytes;
        peak_ = std::max(peak_, live_);
        if (live_ > budget_) {
            overflowed_ = true;
        }
    }

    void Release(uint64_t bytes) {
        live_ = (bytes > live_) ? 0 : (live_ - bytes);
    }

    bool CanAcquire(uint64_t bytes) const {
        return (live_ + bytes) <= budget_;
    }

    uint64_t Budget() const { return budget_; }
    uint64_t Remaining() const { return (budget_ > live_) ? (budget_ - live_) : 0; }
    bool Overflowed() const { return overflowed_; }
    uint64_t Peak() const { return peak_; }
    uint64_t Live() const { return live_; }

private:
    uint64_t budget_ = 0;
    uint64_t live_ = 0;
    uint64_t peak_ = 0;
    bool overflowed_ = false;
};
