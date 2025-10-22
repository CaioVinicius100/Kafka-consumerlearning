// FmtBrokerConsumerFactory.hpp — estilo semelhante ao FmtBrokerMessage
#pragma once

// STD
#include <memory>
#include <string>

// Kafka
#include <cppkafka/cppkafka.h>

// Local
#include "FmtBrokerConsumer.hpp"

class FmtBrokerConsumerFactory {
public:
    // Lê FmtBroker.cfg (mesmo formato do Producer) e instancia o consumer
    static std::unique_ptr<FmtBrokerConsumer> create(const std::string& cfg_path);

private:
    static cppkafka::Configuration load_config(const std::string& path, std::string& topic_out);
};
