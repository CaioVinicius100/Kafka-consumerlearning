#include "KafkaTestConsumer.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <vector>

#include <cppkafka/exceptions.h>

namespace {
std::string trim(const std::string& value) {
    std::string::size_type start = 0;
    while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start]))) {
        ++start;
    }
    std::string::size_type end = value.size();
    while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
        --end;
    }
    return value.substr(start, end - start);
}
}

KafkaTestConsumer::KafkaTestConsumer()
    : enable_auto_commit_(true), running_(false) {
}

KafkaTestConsumer::KafkaTestConsumer(const std::string& config_path)
    : config_path_(config_path), enable_auto_commit_(true), running_(false) {
}

KafkaTestConsumer::~KafkaTestConsumer() {
    stop();
    if (consumer_) {
        try {
            consumer_->unsubscribe();
            consumer_->close();
        }
        catch (const std::exception& ex) {
            std::cerr << "Error while closing consumer: " << ex.what() << std::endl;
        }
        consumer_.reset();
    }
}

void KafkaTestConsumer::setConfigPath(const std::string& config_path) {
    config_path_ = config_path;
}

void KafkaTestConsumer::loadConfig() {
    if (config_path_.empty()) {
        throw std::runtime_error("Configuration path is not set");
    }

    std::ifstream config_file(config_path_.c_str());
    if (!config_file.is_open()) {
        throw std::runtime_error("Failed to open configuration file: " + config_path_);
    }

    bootstrap_servers_.clear();
    group_id_.clear();
    topic_name_.clear();
    auto_offset_reset_.clear();
    enable_auto_commit_ = true;

    std::string line;
    while (std::getline(config_file, line)) {
        parseConfigLine(line);
    }

    if (bootstrap_servers_.empty()) {
        throw std::runtime_error("Missing bootstrap.servers in configuration");
    }
    if (group_id_.empty()) {
        throw std::runtime_error("Missing group.id in configuration");
    }
    if (topic_name_.empty()) {
        throw std::runtime_error("Missing topic.name in configuration");
    }
    if (auto_offset_reset_.empty()) {
        throw std::runtime_error("Missing auto.offset.reset in configuration");
    }
}

void KafkaTestConsumer::init() {
    if (bootstrap_servers_.empty() || group_id_.empty() || topic_name_.empty() || auto_offset_reset_.empty()) {
        throw std::runtime_error("Configuration values must be loaded before initialization");
    }

    if (consumer_) {
        try {
            consumer_->unsubscribe();
            consumer_->close();
        }
        catch (const std::exception& ex) {
            std::cerr << "Error while reinitializing consumer: " << ex.what() << std::endl;
        }
        consumer_.reset();
    }

    consumer_config_ = cppkafka::Configuration({
        {"bootstrap.servers", bootstrap_servers_},
        {"group.id", group_id_},
        {"auto.offset.reset", auto_offset_reset_},
        {"enable.auto.commit", enable_auto_commit_ ? "true" : "false"}
    });

    consumer_.reset(new cppkafka::Consumer(consumer_config_));

    std::vector<std::string> topics;
    topics.push_back(topic_name_);
    consumer_->subscribe(topics);
}

void KafkaTestConsumer::start() {
    if (!consumer_) {
        throw std::runtime_error("Consumer is not initialized");
    }

    running_.store(true);

    while (running_.load()) {
        try {
            cppkafka::Message message = consumer_->poll(std::chrono::milliseconds(500));
            if (!running_.load()) {
                break;
            }
            if (!message) {
                continue;
            }

            if (message.get_error()) {
                if (message.is_eof()) {
                    continue;
                }
                std::cerr << "Kafka error: " << message.get_error().to_string() << std::endl;
                continue;
            }

            cppkafka::Buffer payload_buffer = message.get_payload();
            if (!payload_buffer || payload_buffer.get_size() == 0) {
                std::cout << "Received empty payload from partition "
                          << message.get_partition() << " at offset "
                          << message.get_offset() << std::endl;
                continue;
            }

            std::string payload;
            payload.assign(static_cast<const char*>(payload_buffer.get_data()),
                           payload_buffer.get_size());

            std::string key;
            cppkafka::Buffer key_buffer = message.get_key();
            if (key_buffer && key_buffer.get_size() > 0) {
                key.assign(static_cast<const char*>(key_buffer.get_data()),
                           key_buffer.get_size());
            }

            std::cout << "Received message partition=" << message.get_partition()
                      << " offset=" << message.get_offset();
            if (!key.empty()) {
                std::cout << " key=" << key;
            }
            else {
                std::cout << " key=<none>";
            }
            std::cout << std::endl;
            std::cout << payload << std::endl;
        }
        catch (const cppkafka::Exception& ex) {
            std::cerr << "Polling exception: " << ex.what() << std::endl;
        }
    }

    if (consumer_) {
        try {
            consumer_->unsubscribe();
            consumer_->close();
        }
        catch (const std::exception& ex) {
            std::cerr << "Error while closing consumer: " << ex.what() << std::endl;
        }
        consumer_.reset();
    }
}

void KafkaTestConsumer::stop() {
    running_.store(false);
}

void KafkaTestConsumer::parseConfigLine(const std::string& line) {
    std::string trimmed = trim(line);
    if (trimmed.empty()) {
        return;
    }
    if (trimmed[0] == '#' || trimmed[0] == ';') {
        return;
    }

    std::string::size_type equals_position = trimmed.find('=');
    if (equals_position == std::string::npos) {
        return;
    }

    std::string key = trim(trimmed.substr(0, equals_position));
    std::string value = trim(trimmed.substr(equals_position + 1));

    if (key == "bootstrap.servers") {
        bootstrap_servers_ = value;
    }
    else if (key == "group.id") {
        group_id_ = value;
    }
    else if (key == "topic.name") {
        topic_name_ = value;
    }
    else if (key == "auto.offset.reset") {
        auto_offset_reset_ = value;
    }
    else if (key == "enable.auto.commit") {
        enable_auto_commit_ = parseBool(value);
    }
}

bool KafkaTestConsumer::parseBool(const std::string& value) {
    std::string lowered = value;
    for (std::string::size_type i = 0; i < lowered.size(); ++i) {
        lowered[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(lowered[i])));
    }
    if (lowered == "true" || lowered == "1" || lowered == "yes" || lowered == "on") {
        return true;
    }
    if (lowered == "false" || lowered == "0" || lowered == "no" || lowered == "off") {
        return false;
    }
    throw std::runtime_error("Invalid boolean value in configuration: " + value);
}
