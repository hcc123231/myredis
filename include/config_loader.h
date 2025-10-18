#pragma once
#include"config.h"
namespace myredis{
    bool loadConfigFromFile(const std::string &path, ServerConfig &cfg, std::string &err);
}