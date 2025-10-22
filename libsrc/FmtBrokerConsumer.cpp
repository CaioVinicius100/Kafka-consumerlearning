// FmtBrokerConsumer.cpp
// STD
#include <iostream>
#include <sstream>

// Local
#include "FmtBrokerConsumer.hpp"

FmtBrokerConsumer::FmtBrokerConsumer(const cppkafka::Configuration& cfg, const std::string& topic)
    : consumer_{cfg}, topic_{topic} {
    consumer_.subscribe({topic_});
}

void FmtBrokerConsumer::stop() {
    stop_.store(true);
}

void FmtBrokerConsumer::handleJson(const std::string& key, const std::string& payload) {
    try {
        auto jv = boost::json::parse(payload);
        const auto& obj = jv.as_object();

        // Campos típicos do FmtBrokerMessage — ajuste se necessário
        auto get = [&](const char* k)->std::string {
            auto it = obj.if_contains(k);
            if (!it) return {};
            if (it->is_string()) return boost::json::value_to<std::string>(*it);
            return boost::json::serialize(*it);
        };

        const std::string network  = get("network");
        const std::string mti      = get("mti");
        const std::string resp     = get("response_code");
        const std::string decision = get("decision");

        // Trocar por OTraceInfo/syslg se preferir
        std::cout << "[CONSUMED] key=" << key
                  << " network=" << network
                  << " mti=" << mti
                  << " resp=" << resp
                  << " decision=" << decision << std::endl;

        // TODO: acionar rotinas IST (persistência/auditoria), se desejado

    } catch (const std::exception& ex) {
        std::cerr << "[JSON_ERROR] " << ex.what() << std::endl;
    }
}

int FmtBrokerConsumer::run(int max_messages, int poll_timeout_ms) {
    int consumed = 0;
    while (!stop_.load()) {
        auto msg = consumer_.poll(std::chrono::milliseconds(poll_timeout_ms));
        if (!msg) continue;

        if (msg.get_error()) {
            if (msg.is_eof()) {
                // Partição chegou ao EOF (útil em DEV)
                continue;
            } else {
                std::cerr << "[KAFKA_ERROR] " << msg.get_error() << std::endl;
                continue;
            }
        }

        const std::string key     = msg.get_key() ? msg.get_key() : std::string{};
        const std::string payload = msg.get_payload() ? std::string(msg.get_payload()) : std::string{};

        // Processamento do JSON
        handleJson(key, payload);

        // Commit manual (respeita enable.auto.commit=false)
        try { consumer_.commit(msg); } catch (...) {}

        ++consumed;
        if (max_messages > 0 && consumed >= max_messages) break;
    }

    try { consumer_.unsubscribe(); } catch (...) {}
    try { consumer_.close(); } catch (...) {}
    return consumed;
}
