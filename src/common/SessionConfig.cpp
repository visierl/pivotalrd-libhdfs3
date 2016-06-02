/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "Exception.h"
#include "ExceptionInternal.h"
#include "Function.h"
#include "SessionConfig.h"

#include <sstream>

#define ARRAYSIZE(A) (sizeof(A) / sizeof(A[0]))

#define ALIAS true
#define ORIG false

namespace Hdfs {
namespace Internal {

template<typename T>
static void CheckRangeGE(const char * key, T const & value, T const & target) {
    if (!(value >= target)) {
        std::stringstream ss;
        ss.imbue(std::locale::classic());
        ss << "Invalid configure item: \"" << key << "\", value: " << value
           << ", expected value should be larger than " << target;
        THROW(HdfsConfigInvalid, "%s", ss.str().c_str());
    }
}

template<typename T>
static void CheckMultipleOf(const char * key, const T & value, int unit) {
    if (value <= 0 || value % unit != 0) {
        THROW(HdfsConfigInvalid, "%s should be larger than 0 and be the multiple of %d.", key, unit);
    }
}

SessionConfig::SessionConfig(const Config & conf) {
    ConfigDefault<bool> boolValues [] = {
        {
            &rpcTcpNoDelay, "rpc.client.connect.tcpnodelay", true, ORIG
        }, {
            &readFromLocal, "dfs.client.read.shortcircuit", true, ORIG
        }, {
            &addDatanode, "output.replace-datanode-on-failure", true, ORIG
        }, {
            &notRetryAnotherNode, "input.notretry-another-node", false, ORIG
        }, {
            &useMappedFile, "input.localread.mappedfile", false, ORIG
        }, {
            &legacyLocalBlockReader, "dfs.client.use.legacy.blockreader.local", false, ORIG
        }
    };
    ConfigDefault<int32_t> i32Values[] = {
        {
            &rpcMaxIdleTime, "rpc.client.max.idle", 10 * 1000, ORIG, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &rpcPingTimeout, "rpc.client.ping.interval", 10 * 1000, ORIG
        }, {
            &rpcConnectTimeout, "rpc.client.connect.timeout", 600 * 1000, ORIG
        }, {
            &rpcReadTimeout, "rpc.client.read.timeout", 3600 * 1000, ORIG
        }, {
            &rpcWriteTimeout, "rpc.client.write.timeout", 3600 * 1000, ORIG
        }, {
            &rpcSocketLingerTimeout, "rpc.client.socekt.linger.timeout", -1, ORIG
        }, {
            &rpcMaxRetryOnConnect, "rpc.client.connect.retry", 10, ORIG, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &rpcTimeout, "rpc.client.timeout", 3600 * 1000, ORIG
        }, {
          &defaultReplica, "dfs.default.replica", 3, ORIG, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &inputConnTimeout, "input.connect.timeout", 600 * 1000, ORIG
        }, {
            &inputReadTimeout, "input.read.timeout", 3600 * 1000, ORIG
        }, {
            &inputWriteTimeout, "input.write.timeout", 3600 * 1000, ORIG
        }, {
            &localReadBufferSize, "input.localread.default.buffersize", 1 * 1024 * 1024, ORIG, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &prefetchSize, "dfs.prefetchsize", 10, ORIG, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &maxGetBlockInfoRetry, "input.read.getblockinfo.retry", 3, ORIG, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &maxLocalBlockInfoCacheSize, "input.localread.blockinfo.cachesize", 1000, ORIG, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &maxReadBlockRetry, "input.read.max.retry", 60, ORIG, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &chunkSize, "output.default.chunksize", 512, ORIG, bind(CheckMultipleOf<int32_t>, _1, _2, 512)
        }, {
            &packetSize, "output.default.packetsize", 64 * 1024, ORIG
        }, {
            &blockWriteRetry, "output.default.write.retry", 10, ORIG, bind(CheckRangeGE<int32_t>, _1, _2, 1)
        }, {
            &outputConnTimeout, "output.connect.timeout", 600 * 1000, ORIG
        }, {
            &outputReadTimeout, "output.read.timeout", 3600 * 1000, ORIG
        }, {
            &outputWriteTimeout, "output.write.timeout", 3600 * 1000, ORIG
        }, {
            &closeFileTimeout, "output.close.timeout", 3600 * 1000, ORIG
        }, {
            &packetPoolSize, "output.packetpool.size", 1024, ORIG
        }, {
            &heartBeatInterval, "output.heeartbeat.interval", 10 * 1000, ORIG
        }, {
            &rpcMaxHARetry, "dfs.client.failover.max.attempts", 15, ORIG, bind(CheckRangeGE<int32_t>, _1, _2, 0)
        }, {
          &maxFileDescriptorCacheSize, "dfs.client.read.shortcircuit.streams.cache.size", 256, ORIG, bind(CheckRangeGE<int32_t>, _1, _2, 0)
        }, {
            &socketCacheExpiry, "dfs.client.socketcache.expiryMsec", 3000, ORIG, bind(CheckRangeGE<int32_t>, _1, _2, 0)
        }, {
            &socketCacheCapacity, "dfs.client.socketcache.capacity", 16, ORIG, bind(CheckRangeGE<int32_t>, _1, _2, 0)
        }
    };
    ConfigDefault<int64_t> i64Values [] = {
        {
            &defaultBlockSize, "dfs.default.blocksize", 64 * 1024 * 1024, ORIG, bind(CheckMultipleOf<int64_t>, _1, _2, 512)
        }
    };
    ConfigDefault<std::string> strValues [] = {
        // fs.defaultFS has two aliases, it must come first so that
        // the default value will get set if it is not found and the
        // aliases can skip setting the default.
        {&defaultFS, "fs.defaultFS", "hdfs://localhost:9000", ORIG },
        {&defaultFS, "dfs.default.uri", "hdfs://localhost:9000", ALIAS },  // fs.defaultFS
        {&defaultFS, "fs.default.name", "hdfs://localhost:9000", ALIAS }, // fs.defaultFS
        {&rpcAuthMethod, "hadoop.security.authentication", "simple", ORIG },
        {&kerberosCachePath, "hadoop.security.kerberos.ticket.cache.path", "", ORIG },
        {&logSeverity, "dfs.client.log.severity", "INFO", ORIG },
        {&domainSocketPath, "dfs.domain.socket.path", "", ORIG}
    };

    for (size_t i = 0; i < ARRAYSIZE(boolValues); ++i) {
        if (!boolValues[i].alias) {
            *boolValues[i].variable = conf.getBool(boolValues[i].key,
                                                   boolValues[i].value);
        } else {
            // For aliases, assume that the default value has already been
            // picked up and is correct.  If a non-default value is given,
            // take that one.
            try {
                *boolValues[i].variable = conf.getBool(boolValues[i].key);
            } catch (const HdfsConfigNotFound & e) {
                continue;
            }
        }

        if (boolValues[i].check) {
            boolValues[i].check(boolValues[i].key, *boolValues[i].variable);
        }
    }

    for (size_t i = 0; i < ARRAYSIZE(i32Values); ++i) {
        if (!i32Values[i].alias) {
            *i32Values[i].variable = conf.getInt32(i32Values[i].key,
                                                   i32Values[i].value);
        } else {
            // For aliases, assume that the default value has already been
            // picked up and is correct.  If a non-default value is given,
            // take that one.
            try {
                *i32Values[i].variable = conf.getInt32(i32Values[i].key);
            } catch (const HdfsConfigNotFound & e) {
                continue;
            }
        }

        if (i32Values[i].check) {
            i32Values[i].check(i32Values[i].key, *i32Values[i].variable);
        }
    }

    for (size_t i = 0; i < ARRAYSIZE(i64Values); ++i) {
        if (!i64Values[i].alias) {
            *i64Values[i].variable = conf.getInt64(i64Values[i].key,
                                                   i64Values[i].value);
        } else {
            // For aliases, assume that the default value has already been
            // picked up and is correct.  If a non-default value is given,
            // take that one.
            try {
                *i64Values[i].variable = conf.getInt64(i64Values[i].key);
            } catch (const HdfsConfigNotFound & e) {
                continue;
            }
        }

        if (i64Values[i].check) {
            i64Values[i].check(i64Values[i].key, *i64Values[i].variable);
        }
    }

    for (size_t i = 0; i < ARRAYSIZE(strValues); ++i) {
        if (!strValues[i].alias) {
            *strValues[i].variable = conf.getString(strValues[i].key,
                                                    strValues[i].value);
        } else {
            // For aliases, assume that the default value has already been
            // picked up and is correct.  If a non-default value is given,
            // take that one.
            try {
                *strValues[i].variable = conf.getString(strValues[i].key);
            } catch (const HdfsConfigNotFound & e) {
                continue;
            }
        }

        if (strValues[i].check) {
            strValues[i].check(strValues[i].key, *strValues[i].variable);
        }
    }
}

}
}
