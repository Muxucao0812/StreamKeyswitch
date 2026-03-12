#pragma once

#include "model/he_params.h"

#include <string>

struct HEParamsLoadResult {
    bool ok = false;
    HEParams params;
    std::string source;
    std::string error_message;
};

HEParamsLoadResult LoadHEParamsFromFile(const std::string& path);
