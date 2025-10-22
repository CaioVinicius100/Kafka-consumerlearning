// FmtBrokerConsumerMain.cpp — binário de teste opcional
// STD
#include <iostream>
#include <csignal>

// Local
#include "FmtBrokerConsumerFactory.hpp"

static FmtBrokerConsumer* g_consumer = nullptr;

static void sigint_handler(int) {
    if (g_consumer) g_consumer->stop();
}

int main() {
    try {
        auto consumer = FmtBrokerConsumerFactory::create("FmtBroker.cfg");
        g_consumer = consumer.get();
        std::signal(SIGINT, sigint_handler);

        std::cout << "[INFO] Consumindo... (Ctrl+C para sair)" << std::endl;
        int n = consumer->run(/*max_messages=*/0, /*poll_timeout_ms=*/100);
        std::cout << "[INFO] Total consumido: " << n << std::endl;
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "[ERROR] " << ex.what() << std::endl;
        return 1;
    }
}
