/**
 * kafkadb.cpp
 * This file is part of the YATE Project http://YATE.null.ro
 *
 * Kafka DB module tests.
 *
 * Yet Another Telephony Engine - a fully featured software PBX and IVR
 * Copyright (C) 2023 Null Team
 *
 * This software is distributed under multiple licenses;
 * see the COPYING file in the main directory for licensing
 * information for this specific distribution.
 *
 * This use of this software may be subject to additional restrictions.
 * See the LEGAL file in the main directory for details.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 */

#include <yatengine.h>

#include <string.h>

using namespace TelEngine;

class TestKafkaDb : public Plugin
{
public:
    TestKafkaDb();
    virtual void initialize();
private:
    void report(const char* test, bool ok, const char* detail);
};

TestKafkaDb::TestKafkaDb()
    : Plugin("testkafkadb")
{
    Output("Hello, I am module TestKafkaDb");
}

void TestKafkaDb::report(const char* test, bool ok, const char* detail)
{
    if (ok)
	Debug(test,DebugInfo,"%s",detail);
    else
	Debug(test,DebugWarn,"%s",detail);
}

void TestKafkaDb::initialize()
{
    Output("Initializing module TestKafkaDb");

    String modulePath = Engine::modulePath();
    modulePath << PATH_SEP << "server" << PATH_SEP << "kafkadb.yate";
    bool loaded = Engine::loadPlugin(modulePath.c_str(),true,false);
    report("kafkadb-load",loaded,loaded ? "Loaded kafkadb module" : "Failed to load kafkadb module");
    if (!loaded)
	return;

    Message msg("kafka.send");
    msg.setParam("account","unit_test");
    msg.setParam("topic","unit-test-topic");
    msg.setParam("payload","test-payload");
    Engine::dispatch(msg);

#ifdef HAVE_RDKAFKA
    const char* expect = "unknown_account";
#else
    const char* expect = "librdkafka_not_available";
#endif
    const char* err = msg.getValue("error");
    bool ok = (err && !::strcmp(err,expect));
    String detail;
    detail << "Expected error '" << expect << "' got '" << (err ? err : "<none>") << "'";
    report("kafkadb-dispatch",ok,detail.c_str());
}

INIT_PLUGIN(TestKafkaDb);

/* vi: set ts=8 sw=4 sts=4 noet: */
