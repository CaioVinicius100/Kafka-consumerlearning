// FmtBrokerConsumer.hpp — estilo semelhante ao FmtBrokerMessage (apenas escrita/organização)
#pragma once

// STD
#include <string>
#include <atomic>

// JSON
#include <boost/json.hpp>

// Kafka
#include <cppkafka/cppkafka.h>

class FmtBrokerConsumer {
public:
    FmtBrokerConsumer(const cppkafka::Configuration& cfg, const std::string& topic);

    // Loop de consumo; max_messages=0 => ilimitado; retorna total consumido
    int run(int max_messages = 0, int poll_timeout_ms = 100);

    // Sinaliza parada graciosa
    void stop();

private:
    // Handler principal de processamento do payload JSON
    void handleJson(const std::string& key, const std::string& payload);

private:
    cppkafka::Consumer consumer_;
    std::string topic_;
    std::atomic<bool> stop_{false};
};
