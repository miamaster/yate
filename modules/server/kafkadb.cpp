/**
 * kafkadb.cpp
 * This file is part of the YATE Project http://YATE.null.ro
 *
 * Kafka integration module for Yate.
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

#include <yatephone.h>

#include <string.h>

#ifdef HAVE_RDKAFKA
#include <rdkafka/rdkafka.h>
#endif

using namespace TelEngine;
namespace { // anonymous

class KafkaAccount;

static ObjList s_accounts;
Mutex s_conmutex(false,"Kafka::acc");
static unsigned int s_failedConns;

class KafkaAccount : public RefObject, public Mutex
{
public:
    KafkaAccount(const NamedList& sect);
    ~KafkaAccount();

    bool initProducer();
    void dropProducer();
    bool send(Message& msg, String& error);
    bool hasConn();
    virtual const String& toString() const
	{ return m_name; }

    inline uint64_t total() const
	{ return m_totalMessages; }
    inline uint64_t failed() const
	{ return m_failedMessages; }

private:
    String m_name;
    bool m_enabled;
    String m_brokers;
    String m_clientId;
    String m_topic;
    String m_acks;
    String m_compression;
    String m_securityProtocol;
    String m_saslMechanism;
    String m_saslUsername;
    String m_saslPassword;
    String m_sslCaLocation;
    String m_sslCertificateLocation;
    String m_sslKeyLocation;
    String m_sslKeyPassword;
    unsigned int m_lingerMs;
    unsigned int m_batchSize;
    unsigned int m_messageTimeoutMs;
    unsigned int m_requestTimeoutMs;
    unsigned int m_sendRetries;
    unsigned int m_flushTimeoutMs;

#ifdef HAVE_RDKAFKA
    rd_kafka_t* m_producer;
#endif

    Mutex m_statsMutex;
    uint64_t m_totalMessages;
    uint64_t m_failedMessages;
};

class KafkaModule : public Module
{
public:
    KafkaModule();
    ~KafkaModule();
protected:
    virtual void initialize();
    virtual void statusModule(String& str);
    virtual void statusParams(String& str);
    virtual void statusDetail(String& str);
    virtual void genUpdate(Message& msg);
private:
    bool m_init;
};

static KafkaModule module;

class KafkaHandler : public MessageHandler
{
public:
    KafkaHandler(unsigned int prio = 100)
	: MessageHandler("kafka.send",prio,module.name())
	{ }
    virtual bool received(Message& msg);
};

#ifdef HAVE_RDKAFKA
static void kafkaDeliveryReport(rd_kafka_t* rk, const rd_kafka_message_t* msg, void* opaque)
{
    if (!msg)
	return;
    if (msg->err)
	Debug(&module,DebugWarn,"Kafka delivery failed: %s",rd_kafka_err2str(msg->err));
    else
	XDebug(&module,DebugAll,"Kafka message delivered (partition %d, offset %lld)",
	    msg->partition,(long long)msg->offset);
}
#endif

KafkaAccount::KafkaAccount(const NamedList& sect)
    : Mutex(true,"KafkaAccount"),
      m_name(sect),
      m_enabled(sect.getBoolValue("enabled",true)),
      m_brokers(sect.getValue("brokers")),
      m_clientId(sect.getValue("client_id","yate")),
      m_topic(sect.getValue("topic")),
      m_acks(sect.getValue("acks","all")),
      m_compression(sect.getValue("compression")),
      m_securityProtocol(sect.getValue("security_protocol")),
      m_saslMechanism(sect.getValue("sasl_mechanism")),
      m_saslUsername(sect.getValue("sasl_username")),
      m_saslPassword(sect.getValue("sasl_password")),
      m_sslCaLocation(sect.getValue("ssl_ca_location")),
      m_sslCertificateLocation(sect.getValue("ssl_certificate_location")),
      m_sslKeyLocation(sect.getValue("ssl_key_location")),
      m_sslKeyPassword(sect.getValue("ssl_key_password")),
      m_lingerMs(sect.getIntValue("linger_ms",0)),
      m_batchSize(sect.getIntValue("batch_size",0)),
      m_messageTimeoutMs(sect.getIntValue("message_timeout_ms",0)),
      m_requestTimeoutMs(sect.getIntValue("request_timeout_ms",0)),
      m_sendRetries(sect.getIntValue("send_retries",0)),
      m_flushTimeoutMs(sect.getIntValue("flush_timeout_ms",0)),
#ifdef HAVE_RDKAFKA
      m_producer(0),
#endif
      m_statsMutex(false,"KafkaStats"),
      m_totalMessages(0),
      m_failedMessages(0)
{
}

KafkaAccount::~KafkaAccount()
{
    dropProducer();
}

void KafkaAccount::dropProducer()
{
#ifdef HAVE_RDKAFKA
    if (!m_producer)
	return;
    rd_kafka_flush(m_producer,2000);
    rd_kafka_destroy(m_producer);
    m_producer = 0;
#endif
}

bool KafkaAccount::initProducer()
{
#ifndef HAVE_RDKAFKA
    return false;
#else
    if (m_producer)
	return true;
    if (!m_enabled) {
	Debug(&module,DebugNote,"Kafka account '%s' is disabled",m_name.c_str());
	return false;
    }
    if (m_brokers.null()) {
	Alarm(&module,DebugWarn,"Kafka account '%s' has no brokers configured",m_name.c_str());
	return false;
    }
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    char errstr[512];

    if (!m_clientId.null())
	rd_kafka_conf_set(conf,"client.id",m_clientId.c_str(),0,0);
    rd_kafka_conf_set(conf,"bootstrap.servers",m_brokers.c_str(),0,0);
    if (!m_acks.null())
	rd_kafka_conf_set(conf,"acks",m_acks.c_str(),0,0);
    if (!m_compression.null())
	rd_kafka_conf_set(conf,"compression.type",m_compression.c_str(),0,0);
    if (m_lingerMs)
	rd_kafka_conf_set(conf,"linger.ms",String(m_lingerMs).c_str(),0,0);
    if (m_batchSize)
	rd_kafka_conf_set(conf,"batch.size",String(m_batchSize).c_str(),0,0);
    if (m_messageTimeoutMs)
	rd_kafka_conf_set(conf,"message.timeout.ms",String(m_messageTimeoutMs).c_str(),0,0);
    if (m_requestTimeoutMs)
	rd_kafka_conf_set(conf,"request.timeout.ms",String(m_requestTimeoutMs).c_str(),0,0);
    if (!m_securityProtocol.null())
	rd_kafka_conf_set(conf,"security.protocol",m_securityProtocol.c_str(),0,0);
    if (!m_saslMechanism.null())
	rd_kafka_conf_set(conf,"sasl.mechanism",m_saslMechanism.c_str(),0,0);
    if (!m_saslUsername.null())
	rd_kafka_conf_set(conf,"sasl.username",m_saslUsername.c_str(),0,0);
    if (!m_saslPassword.null())
	rd_kafka_conf_set(conf,"sasl.password",m_saslPassword.c_str(),0,0);
    if (!m_sslCaLocation.null())
	rd_kafka_conf_set(conf,"ssl.ca.location",m_sslCaLocation.c_str(),0,0);
    if (!m_sslCertificateLocation.null())
	rd_kafka_conf_set(conf,"ssl.certificate.location",m_sslCertificateLocation.c_str(),0,0);
    if (!m_sslKeyLocation.null())
	rd_kafka_conf_set(conf,"ssl.key.location",m_sslKeyLocation.c_str(),0,0);
    if (!m_sslKeyPassword.null())
	rd_kafka_conf_set(conf,"ssl.key.password",m_sslKeyPassword.c_str(),0,0);

    rd_kafka_conf_set_dr_msg_cb(conf,kafkaDeliveryReport);

    m_producer = rd_kafka_new(RD_KAFKA_PRODUCER,conf,errstr,sizeof(errstr));
    if (!m_producer) {
	Alarm(&module,DebugWarn,"Failed to create Kafka producer for '%s': %s",
	    m_name.c_str(),errstr);
	rd_kafka_conf_destroy(conf);
	return false;
    }
    return true;
#endif
}

bool KafkaAccount::send(Message& msg, String& error)
{
#ifndef HAVE_RDKAFKA
    error = "librdkafka_not_available";
    return false;
#else
    if (!initProducer()) {
	error = "producer_not_ready";
	return false;
    }
    const char* topic = msg.getValue("topic",m_topic);
    if (TelEngine::null(topic)) {
	error = "missing_topic";
	return false;
    }
    const char* payload = msg.getValue("payload");
    if (TelEngine::null(payload))
	payload = msg.getValue("text");
    if (TelEngine::null(payload))
	payload = msg.getValue("message");
    if (TelEngine::null(payload))
	payload = msg.getValue("data");
    if (TelEngine::null(payload)) {
	error = "missing_payload";
	return false;
    }
    const char* key = msg.getValue("key");
    int partition = msg.getIntValue("partition",RD_KAFKA_PARTITION_UA);
    size_t payloadLen = (size_t)msg.getIntValue("payload_len",(int)strlen(payload));
    size_t keyLen = key ? strlen(key) : 0;
    unsigned int retries = m_sendRetries;
    if (msg.getParam("send_retries"))
	retries = msg.getIntValue("send_retries",retries);

    bool sent = false;
    for (unsigned int attempt = 0; attempt <= retries; attempt++) {
	rd_kafka_topic_t* rkt = rd_kafka_topic_new(m_producer,topic,0);
	if (!rkt) {
	    error = "topic_create_failed";
	    break;
	}
	if (rd_kafka_produce(rkt,partition,RD_KAFKA_MSG_F_COPY,
		    (void*)payload,payloadLen,key,keyLen,0) == -1) {
	    rd_kafka_resp_err_t err = rd_kafka_last_error();
	    rd_kafka_topic_destroy(rkt);
	    if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL && attempt < retries) {
		rd_kafka_poll(m_producer,50);
		continue;
	    }
	    error = rd_kafka_err2str(err);
	    break;
	}
	rd_kafka_topic_destroy(rkt);
	rd_kafka_poll(m_producer,0);
	sent = true;
	break;
    }

    unsigned int flushTimeout = m_flushTimeoutMs;
    if (msg.getParam("flush_timeout_ms"))
	flushTimeout = msg.getIntValue("flush_timeout_ms",flushTimeout);
    if (sent && flushTimeout)
	rd_kafka_flush(m_producer,flushTimeout);

    Lock lck(m_statsMutex);
    m_totalMessages++;
    if (!sent)
	m_failedMessages++;
    return sent;
#endif
}

bool KafkaAccount::hasConn()
{
#ifdef HAVE_RDKAFKA
    return m_producer != 0;
#else
    return false;
#endif
}

static KafkaAccount* findAccount(const String& account)
{
    if (account.null())
	return 0;
    return static_cast<KafkaAccount*>(s_accounts[account]);
}

bool KafkaHandler::received(Message& msg)
{
    const String* account = msg.getParam("account");
    if (TelEngine::null(account))
	account = msg.getParam("connection");
    String useAccount = account ? *account : String("default");
    s_conmutex.lock();
    RefPointer<KafkaAccount> acc = findAccount(useAccount);
    s_conmutex.unlock();
    if (!acc) {
	msg.setParam("error","unknown_account");
	return false;
    }
    String error;
    bool ok = acc->send(msg,error);
    msg.setParam("kafka.account",acc->toString());
    if (!ok)
	msg.setParam("error",error);
    return ok;
}

KafkaModule::KafkaModule()
    : Module("kafkadb","kafka",true),
      m_init(false)
{
    Output("Loaded module Kafka");
}

KafkaModule::~KafkaModule()
{
    Output("Unloading module Kafka");
    s_accounts.clear();
}

void KafkaModule::statusModule(String& str)
{
    Module::statusModule(str);
    str.append("format=Total|Failed",",");
}

void KafkaModule::statusParams(String& str)
{
    s_conmutex.lock();
    str.append("conns=",",") << s_accounts.count();
    str.append("failed=",",") << s_failedConns;
    s_conmutex.unlock();
}

void KafkaModule::statusDetail(String& str)
{
    s_conmutex.lock();
    for (ObjList* o = s_accounts.skipNull(); o; o = o->skipNext()) {
	KafkaAccount* acc = static_cast<KafkaAccount*>(o->get());
	str.append(acc->toString().c_str(),",") << "=" << acc->total() << "|" << acc->failed();
    }
    s_conmutex.unlock();
}

void KafkaModule::initialize()
{
    Module::initialize();
    if (m_init)
	return;
#ifndef HAVE_RDKAFKA
    Alarm(this,DebugWarn,"Kafka support disabled: build with librdkafka");
    return;
#else
    Output("Initializing module Kafka");
    Configuration cfg(Engine::configFile("kafkadb"));
    unsigned int i;
    for (i = 0; i < cfg.sections(); i++) {
	NamedList* sec = cfg.getSection(i);
	if (!sec || (*sec == "general"))
	    continue;
	KafkaAccount* acc = new KafkaAccount(*sec);
	if (sec->getBoolValue("autostart",true) && !acc->initProducer())
	    TelEngine::destruct(acc);
	s_conmutex.lock();
	if (acc) {
	    s_accounts.insert(acc);
	    m_init = true;
	}
	else
	    s_failedConns++;
	s_conmutex.unlock();
    }
    if (m_init)
	Engine::install(new KafkaHandler(cfg.getIntValue("general","priority",100)));
#endif
}

void KafkaModule::genUpdate(Message& msg)
{
    unsigned int index = 0;
    s_conmutex.lock();
    for (ObjList* o = s_accounts.skipNull(); o; o = o->skipNext()) {
	KafkaAccount* acc = static_cast<KafkaAccount*>(o->get());
	msg.setParam(String("kafka.") << index,acc->toString());
	msg.setParam(String("total.") << index,String(acc->total()));
	msg.setParam(String("failed.") << index,String(acc->failed()));
	msg.setParam(String("hasconn.") << index,String::boolText(acc->hasConn()));
	index++;
    }
    s_conmutex.unlock();
    msg.setParam("count",String(index));
}

}; // anonymous namespace

/* vi: set ts=8 sw=4 sts=4 noet: */
