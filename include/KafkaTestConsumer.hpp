#ifndef KAFKA_TEST_CONSUMER_HPP
#define KAFKA_TEST_CONSUMER_HPP

#include <memory>
#include <string>
#include <atomic>

#include <cppkafka/consumer.h>
#include <cppkafka/configuration.h>

class KafkaTestConsumer {
public:
    KafkaTestConsumer();
    explicit KafkaTestConsumer(const std::string& config_path);
    ~KafkaTestConsumer();

    void setConfigPath(const std::string& config_path);

    void loadConfig();
    void init();
    void start();
    void stop();

private:
    void parseConfigLine(const std::string& line);
    static bool parseBool(const std::string& value);

    std::string config_path_;
    std::string bootstrap_servers_;
    std::string group_id_;
    std::string topic_name_;
    std::string auto_offset_reset_;
    bool enable_auto_commit_;

    std::atomic<bool> running_;

    cppkafka::Configuration consumer_config_;
    std::unique_ptr<cppkafka::Consumer> consumer_;
};

#endif // KAFKA_TEST_CONSUMER_HPP
