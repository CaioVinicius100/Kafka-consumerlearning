#include "KafkaTestConsumer.hpp"

#include <csignal>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {
KafkaTestConsumer* g_consumer_instance = 0;

void handleSignal(int signal_number) {
    if (signal_number == SIGINT || signal_number == SIGTERM) {
        if (g_consumer_instance != 0) {
            std::cout << "Signal received, stopping consumer..." << std::endl;
            g_consumer_instance->stop();
        }
    }
}
}

int main(int argc, char** argv) {
    std::string config_path = "cfg/FmtBroker.cfg";
    if (argc > 1) {
        config_path = argv[1];
    }

    KafkaTestConsumer consumer(config_path);
    g_consumer_instance = &consumer;

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    try {
        consumer.loadConfig();
        consumer.init();
        consumer.start();
    }
    catch (const std::exception& ex) {
        std::cerr << "Fatal error: " << ex.what() << std::endl;
        g_consumer_instance = 0;
        consumer.stop();
        return 1;
    }

    g_consumer_instance = 0;
    consumer.stop();
    return 0;
}
