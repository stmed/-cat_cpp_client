/*
 * Copyright (c) 2011-2018, Meituan Dianping. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "router_json_parser.h"

#include "functions.h"
#include "aggregator.h"
#include "server_connection_manager.h"

#include <lib/cat_json.h>
#include <lib/cat_clog.h>
#include <lib/cat_anet.h>

#define ROUTER_CONFIG_ROUTERS "routers"
#define ROUTER_CONFIG_SAMPLE_RATE "sample"
#define ROUTER_CONFIG_BLOCK "block"

extern volatile int g_cat_enabled;

extern int resolveServerIps(char *routerIps);

static int parseJsonStringArray(cJSON *kvsObject, char *objName, int *pNum, sds **pStrArray, char *splitor) {
    cJSON *item = NULL;
    char *itemBuf = NULL;
    item = cJSON_GetObjectItem(kvsObject, objName);
    if (item != NULL && (itemBuf = item->valuestring) != NULL) {
        *pStrArray = catsdssplitlen(itemBuf, strlen(itemBuf), splitor, strlen(splitor), pNum);

        if (*pStrArray != NULL && *pNum > 0) {
            return 1;
        }
    } else {
        INNER_LOG(CLOG_WARNING, "CatRouter Json GetObjectItem Error, no key [%s].", objName);
    }
    return 0;
}

static int parseJsonDouble(cJSON *kvsObject, char *objName, double *pVal) {
    cJSON *item = NULL;
    char *itemBuf = NULL;
    item = cJSON_GetObjectItem(kvsObject, objName);
    if (item != NULL && (itemBuf = item->valuestring) != NULL) {
        char *endBuf = NULL;
        double val = strtod(itemBuf, &endBuf);
        if (endBuf != NULL && endBuf[0] == '\0') {
            *pVal = val;
            return 1;
        } else {
            INNER_LOG(CLOG_WARNING, "CatRouter Json strtod Error, key [%s].", objName);
        }
    } else {
        INNER_LOG(CLOG_WARNING, "CatRouter Json GetObjectItem Error, no key [%s].", objName);
    }
    return 0;
}

static int parsersonInt(cJSON *kvsObject, char *objName, int *pVal) {
    cJSON *item = NULL;
    char *itemBuf = NULL;
    item = cJSON_GetObjectItem(kvsObject, objName);
    if (item != NULL && (itemBuf = item->valuestring) != NULL) {
        if (!catAtoI(itemBuf, 10, pVal)) {
            INNER_LOG(CLOG_WARNING, "CatRouter Json catAtoI Error, key [%s].", objName);
        } else {
            return 1;
        }
    } else {
        INNER_LOG(CLOG_WARNING, "CatRouter Json GetObjectItem Error, no key [%s].", objName);
    }
    return 0;
}

static int parseJsonBool(cJSON *kvsObject, char *objName, int *pRst) {
    cJSON *item = NULL;
    char *itemBuf = NULL;
    item = cJSON_GetObjectItem(kvsObject, objName);
    if (item != NULL && (itemBuf = item->valuestring) != NULL) {
        if (strcmp(itemBuf, "true") == 0 || strcmp(itemBuf, "TRUE") == 0
            || strcmp(itemBuf, "Yes") == 0 || strcmp(itemBuf, "yes") == 0) {
            *pRst = 1;
        } else {
            *pRst = 0;
        }
        return 1;
    } else {
        INNER_LOG(CLOG_WARNING, "CatRouter Json GetObjectItem Error, no key [%s].", objName);
    }
    return 0;
}

static int parseCatJsonRouterItem(cJSON *kvsObject) {
    int rst = 0;

    cJSON *item = NULL;
    char *itemBuf = NULL;

    item = cJSON_GetObjectItem(kvsObject, ROUTER_CONFIG_ROUTERS);
    if (item != NULL && (itemBuf = item->valuestring) != NULL) {
        for (int i = 0; i < strlen(itemBuf); ++i) {
            if (itemBuf[i] == ';') {
                itemBuf[i] = '\0';
                break;
            }
        }
        char destIP[128];
        char c_port[64];
        sds ip = NULL;
        unsigned short port;
        if (resolveIpPortStr(itemBuf, &ip, &port) == 0) {
            if (catAnetResolve(NULL, ip, destIP, 128) == ANET_ERR) {
                INNER_LOG(CLOG_ERROR, "catAnetResolve failed. ip:%s", ip);
                return 0;
            }
            snprintf(c_port, 64, "%d", port);
            strcat(destIP, ":");
            strcat(destIP, c_port);
            itemBuf = destIP;
        }
        INNER_LOG(CLOG_INFO, "ip port after resolve: %s", itemBuf);
        if (resolveServerIps(itemBuf) > 0) {
            rst += 1;
        } else {
            INNER_LOG(CLOG_WARNING, "CatRouter Json catAtoI Error, key [routers].");
        }
    } else {
        INNER_LOG(CLOG_ERROR, "CatRouter Json GetObjectItem Error, no key [routers].");
        return 0;
    }

    double sampleRate = 1.0;
    parseJsonDouble(kvsObject, ROUTER_CONFIG_SAMPLE_RATE, &sampleRate);
    setSampleRate(sampleRate);

    int block = 0;
    parseJsonBool(kvsObject, ROUTER_CONFIG_BLOCK, &block);
    g_cat_enabled = !block;

    return rst;
}

int parseCatJsonRouter(char *jsonBuf) {
    cJSON *catRouterJson = NULL;

    catRouterJson = cJSON_Parse(jsonBuf);
    if (catRouterJson == NULL) {
        INNER_LOG(CLOG_WARNING, "CatRouter Json Parser Error before: [%s]\n", cJSON_GetErrorPtr());
        return 0;
    } else {
        cJSON *kvsItem = cJSON_GetObjectItem(catRouterJson, "kvs");
        if (kvsItem == NULL) {
            INNER_LOG(CLOG_WARNING, "CatRouter Json GetObjectItem [kvs] Error before: [%s]\n", cJSON_GetErrorPtr());
            return 0;
        }
        if (cJSON_GetArraySize(kvsItem) < 1) {
            INNER_LOG(CLOG_WARNING, "CatRouter Json ArraySize [kvs] Error before: [%s]\n", cJSON_GetErrorPtr());
            return 0;
        }

        parseCatJsonRouterItem(kvsItem);
        cJSON_Delete(catRouterJson);
    }
    return 1;
}
