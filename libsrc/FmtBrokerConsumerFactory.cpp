// FmtBrokerConsumerFactory.cpp
// STD
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <stdexcept>
#include <cstdlib>

// Local
#include "FmtBrokerConsumerFactory.hpp"

// trim auxiliar
static inline std::string trim(const std::string& s) {
    auto i = s.find_first_not_of(" \t\r\n");
    if (i == std::string::npos) return "";
    auto j = s.find_last_not_of(" \t\r\n");
    return s.substr(i, j - i + 1);
}

cppkafka::Configuration FmtBrokerConsumerFactory::load_config(const std::string& path, std::string& topic_out) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("Nao foi possivel abrir cfg: " + path);

    std::unordered_map<std::string, std::string> props;
    std::string line;
    while (std::getline(in, line)) {
        auto hash = line.find('#');
        if (hash != std::string::npos) line = line.substr(0, hash);
        line = trim(line);
        if (line.empty()) continue;
        auto eq = line.find('=');
        if (eq == std::string::npos) continue;
        auto k = trim(line.substr(0, eq));
        auto v = trim(line.substr(eq + 1));
        if (!k.empty()) props[k] = v;
    }

    // Campos obrigatórios herdados do FmtBroker
    if (!props.count("bootstrap.servers")) throw std::runtime_error("Config faltando: bootstrap.servers");
    if (!props.count("topic")) throw std::runtime_error("Config faltando: topic");

    topic_out = props["topic"];
    props.erase("topic");

    // Defaults de consumer se não informados no cfg
    if (!props.count("group.id")) props["group.id"] = "fmtbroker-local-test";
    if (!props.count("auto.offset.reset")) props["auto.offset.reset"] = "earliest";
    if (!props.count("enable.auto.commit")) props["enable.auto.commit"] = "false";

    // Converter props -> Configuration
    std::vector<std::pair<std::string,std::string>> kvs;
    kvs.reserve(props.size());
    for (auto& kv : props) kvs.emplace_back(kv.first, kv.second);
    return cppkafka::Configuration{kvs.begin(), kvs.end()};
}

std::unique_ptr<FmtBrokerConsumer> FmtBrokerConsumerFactory::create(const std::string& cfg_path) {
    // Procura FmtBroker.cfg no caminho informado ou em IST_CFG/FmtBroker.cfg
    std::string final_path = cfg_path;
    if (final_path.empty()) {
        if (const char* ist = std::getenv("IST_CFG")) {
            final_path = std::string(ist) + "/FmtBroker.cfg";
        } else {
            final_path = "FmtBroker.cfg";
        }
    }

    std::string topic;
    auto cfg = load_config(final_path, topic);
    return std::unique_ptr<FmtBrokerConsumer>(new FmtBrokerConsumer(cfg, topic));
}
