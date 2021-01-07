//
// Created by Terence on 2018/8/2.
//

#include "cat/client.hpp"

#include <client.h>
#include <ccat/version.h>

using namespace std;

namespace cat {
    // -1: 失败, 0: 成功, 1: 已经初始化过
    int init(const string& domain) {
        return catClientInit(domain.c_str());
    }

    // -1: 失败, 0: 成功, 1: 已经初始化过
    int init(const string& domain, const Config& config) {
        CatClientConfig conf = DEFAULT_CCAT_CONFIG;
        conf.encoderType = config.encoderType;
        conf.enableSampling = config.enableSampling;
        conf.enableMultiprocessing = config.enableMultiprocessing;
        conf.enableHeartbeat = config.enableHeartbeat;
        conf.enableDebugLog = config.enableDebugLog;
        return catClientInitWithConfig(domain.c_str(), &conf);
    }

    string version() {
        return string(CPPCAT_VERSION);
    }

    void destroy() {
        catClientDestroy();
    }
}
