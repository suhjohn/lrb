//
// Created by John Suh on 5/16/20.
//

#ifndef LRB_BLOOM_H
#define LRB_BLOOM_H


#include <fstream>
#include <algorithm>
#include <vector>

#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <bf/bloom_filter/basic.hpp>
#include <bf/bloom_filter/counting.hpp>
#include "filter.h"
#include "bsoncxx/builder/basic/document.hpp"
#include "bsoncxx/json.hpp"

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::sub_array;

using namespace std;

class SetFilter : public Filter {
public:
    SetFilter() : Filter() {}

    void init_with_params(const std::map <std::string, std::string> &params) override {
        for (auto &it: params) {
            if (it.first == "max_n_element") {
                std::istringstream iss(it.second);
                size_t new_n;
                iss >> new_n;
                max_n_element = new_n;
            } else if (it.first == "filter_k") {
                k = stoi(it.second);
            } else {
                cerr << "Set filter unrecognized parameter: " << it.first << endl;
            }
        }
        cerr << "Init Set filter. max_n_element: " << max_n_element << " k: " << k << endl;
        for (int i = 0; i < k; i++) {
            unordered_set <uint64_t> filter;
            filters.push_back(filter);
        }
    }

    size_t total_bytes_used() override {
        return 0;
    }

    void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) override {
        doc.append(kvp("filter_size", std::to_string(total_bytes_used())));
        doc.append(kvp("max_n_element", std::to_string(max_n_element)));
        doc.append(kvp("filter_k", k));
    }

    bool should_filter(SimpleRequest &req) override;

    size_t max_n_element = 40000000;
    uint16_t curr_filter_idx = 0;
    int n_added_obj = 0;
    int k = 2;

    std::vector <unordered_set<uint64_t>> filters;
};

static FilterFactory <SetFilter> factorySetFilter("Set");

class BloomFilter : public Filter {
public:
    BloomFilter() : Filter() {}

    void init_with_params(const std::map <std::string, std::string> &params) override {
        for (auto &it: params) {
            if (it.first == "max_n_element") {
                std::istringstream iss(it.second);
                size_t new_n;
                iss >> new_n;
                max_n_element = new_n;
            } else if (it.first == "bloom_k") {
                k = stoi(it.second);
            } else if (it.first == "fp_rate") {
                fp_rate = stod(it.second);
            } else {
                cerr << "Bloom filter unrecognized parameter: " << it.first << endl;
            }
        }
        cerr << "Init Bloom filter. max_n_element: " << max_n_element << " fp_rate: " << fp_rate << " k: " << k << endl;
        for (int i = 0; i < k; i++) {
            bf::basic_bloom_filter *b = new bf::basic_bloom_filter(fp_rate, max_n_element);
            filters.push_back(b);
        }
    }

    size_t total_bytes_used() override {
        size_t total = 0;
        for (int i = 0; i < k; i++) {
            total += filters[i]->storage().size();
        }
        return total / 8;
    }

    void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) override {
        doc.append(kvp("filter_size", std::to_string(total_bytes_used())));
        doc.append(kvp("max_n_element", std::to_string(max_n_element)));
        doc.append(kvp("fp_rate", std::to_string(fp_rate)));
        doc.append(kvp("bloom_k", k));
        doc.append(kvp("filter_k", k));
    }

    bool should_filter(SimpleRequest &req) override;

    size_t max_n_element = 40000000;
    double fp_rate = 0.001;
    uint16_t curr_filter_idx = 0;
    int n_added_obj = 0;
    int k = 2;

    std::vector<bf::basic_bloom_filter *> filters;
};

static FilterFactory <BloomFilter> factoryBloomFilter("Bloom");

class CountingSetFilter : public Filter {
public:
    CountingSetFilter() : Filter() {}

    void init_with_params(const std::map <std::string, std::string> &params) override {
        for (auto &it: params) {
            if (it.first == "max_n_element") {
                std::istringstream iss(it.second);
                size_t new_n;
                iss >> new_n;
                max_n_element = new_n;
            } else if (it.first == "bloom_k") {
                k = stoi(it.second);
            } else {
                cerr << "CountingSetFilter filter unrecognized parameter: " << it.first << endl;
            }
        }
        cerr << "Init CountingSetFilter filter. max_n_element: " << max_n_element << " k: " << k << endl;

        for (int i = 0; i < k; i++) {
            unordered_map <uint64_t, uint64_t> filter;
            filters.push_back(filter);
        }
    }

    uint64_t count(SimpleRequest &req) {
        auto key = req.get_id();
        for (int i = 0; i < k; i++) {
            if (filters[i].count(key)) {
                return filters[i][key];
            }
        }
        return 0;
    }

    size_t total_bytes_used() override {
        return 0;
    }

    void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) override {
        doc.append(kvp("max_n_element", std::to_string(max_n_element)));
        doc.append(kvp("bloom_k", k));
    }

    bool should_filter(SimpleRequest &req) override;

    size_t max_n_element = 40000000;
    uint16_t curr_filter_idx = 0;
    int n_added_obj = 0;
    int k = 2;
    bool track_kth_hit = false;

    std::vector <unordered_map<uint64_t, uint64_t>> filters;
};

static FilterFactory <CountingSetFilter> factoryCountingSetFilter("CountingSet");

class IgnoringKHitBloomFilter : public Filter {
public:
    IgnoringKHitBloomFilter() : Filter() {}

    void init_with_params(const std::map <std::string, std::string> &params) override {
        for (auto &it: params) {
            if (it.first == "max_n_element") {
                std::istringstream iss(it.second);
                size_t new_n;
                iss >> new_n;
                max_n_element = new_n;
            } else if (it.first == "bloom_k") {
                k = stoi(it.second);
            } else if (it.first == "fp_rate") {
                fp_rate = stod(it.second);
            } else {
                cerr << "Bloom filter unrecognized parameter: " << it.first << endl;
            }
        }
        cerr << "Init Bloom filter. max_n_element: " << max_n_element << " fp_rate: " << fp_rate << " k: " << k << endl;
        for (int i = 0; i < k; i++) {
            bf::basic_bloom_filter *b = new bf::basic_bloom_filter(fp_rate, max_n_element);
            filters.push_back(b);
            unordered_map <uint64_t, uint64_t> count_map;
            count_maps.push_back(count_map);
        }
    }

    size_t total_bytes_used() override {
        size_t total = 0;
        for (int i = 0; i < k; i++) {
            total += filters[i]->storage().size();
        }
        return total / 8;
    }

    void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) override {
        doc.append(kvp("filter_size", std::to_string(total_bytes_used())));
        doc.append(kvp("max_n_element", std::to_string(max_n_element)));
        doc.append(kvp("fp_rate", std::to_string(fp_rate)));
        doc.append(kvp("bloom_k", k));
    }

    bool should_filter(SimpleRequest &req) override;

    size_t max_n_element = 40000000;
    double fp_rate = 0.001;
    uint16_t curr_filter_idx = 0;
    int n_added_obj = 0;
    int k = 2;

    std::vector <unordered_map<uint64_t, uint64_t>> count_maps;
    std::vector<bf::basic_bloom_filter *> filters;
};

static FilterFactory <IgnoringKHitBloomFilter> factoryIgnoringKHitBloomFilter("IgnoringKHitBloom");


class ResettingBloomFilter : public Filter {
public:
    ResettingBloomFilter() : Filter() {}

    void init_with_params(const std::map <std::string, std::string> &params) override {
        for (auto &it: params) {
            if (it.first == "max_n_element") {
                std::istringstream iss(it.second);
                size_t new_n;
                iss >> new_n;
                max_n_element = new_n;
            } else if (it.first == "bloom_k") {
                k = stoi(it.second);
            } else if (it.first == "fp_rate") {
                fp_rate = stod(it.second);
            } else {
                cerr << "Bloom filter unrecognized parameter: " << it.first << endl;
            }
        }
        cerr << "Init Bloom filter. max_n_element: " << max_n_element << " fp_rate: " << fp_rate << " k: " << k << endl;
        for (int i = 0; i < k; i++) {
            bf::basic_bloom_filter *b = new bf::basic_bloom_filter(fp_rate, max_n_element);
            filters.push_back(b);
            unordered_map <uint64_t, uint64_t> count_map;
            count_maps.push_back(count_map);
        }
    }

    size_t total_bytes_used() override {
        size_t total = 0;
        for (int i = 0; i < k; i++) {
            total += filters[i]->storage().size();
        }
        return total / 8;
    }

    void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) override {
        doc.append(kvp("filter_size", std::to_string(total_bytes_used())));
        doc.append(kvp("max_n_element", std::to_string(max_n_element)));
        doc.append(kvp("fp_rate", std::to_string(fp_rate)));
        doc.append(kvp("bloom_k", k));
    }

    bool should_filter(SimpleRequest &req) override;

    size_t max_n_element = 40000000;
    double fp_rate = 0.001;
    uint16_t curr_filter_idx = 0;
    int n_added_obj = 0;
    int k = 2;

    std::vector <unordered_map<uint64_t, uint64_t>> count_maps;
    std::vector<bf::basic_bloom_filter *> filters;
};

static FilterFactory <ResettingBloomFilter> factoryResettingBloomFilter("ResettingBloom");


class KHitCounter {
public:
    CountingSetFilter *filter;
    BloomFilter *bloom_filter;
    int64_t second_hit_byte = 0, unevicted_kth_hit_byte = 0, evicted_kth_hit_byte = 0;

    KHitCounter(const std::map <std::string, std::string> &params) {
        filter = new CountingSetFilter();
        bloom_filter = new BloomFilter();
        filter->init_with_params(params);
        bloom_filter->init_with_params(params);
    }

    void insert(SimpleRequest &req) {
        auto size = req.get_size();
        filter->should_filter(req);
        auto should_filter = bloom_filter->should_filter(req);
        if (!should_filter) {
            if (filter->count(req) == 2) {
                second_hit_byte += size;
            } else if (filter->count(req) > 2) {
                evicted_kth_hit_byte += size;
            }
        }
    }
};

class FPCounter {
public:
    CountingSetFilter *filter;
    BloomFilter *bloom_filter;
    int64_t false_positive = 0, true_positive = 0;

    FPCounter(const std::map <std::string, std::string> &params) {
        filter = new CountingSetFilter();
        bloom_filter = new BloomFilter();
        filter->init_with_params(params);
        bloom_filter->init_with_params(params);
    }

    void insert(SimpleRequest &req) {
        auto size = req.get_size();
        auto should_filter = bloom_filter->should_filter(req);
        if (!should_filter) {
            if (filter->count(req) == 0) {
                false_positive += size;
            } else {
                true_positive += size;
            }
        }
        filter->should_filter(req);
    }
};


class AccessFrequencyCounter {
public:
    unordered_map <uint64_t, uint64_t> count_map;
    vector <int64_t> counter_buckets;
    int bucket_count;

    AccessFrequencyCounter(
            const string &trace_file, uint n_extra_fields,
            int64_t n_early_stop = -1, int _bucket_count = 4) {
        cerr << "init AccessFrequencyCounter" << endl;
        bucket_count = _bucket_count;

        for (int i = 0; i < bucket_count; i++) {
            counter_buckets.push_back(0);
        }

        // count object count until n_early_stop
        ifstream infile(trace_file);
        if (!infile) {
            cerr << "Exception opening/reading file " << trace_file << endl;
            exit(-1);
        }
        int seq = 0;
        uint64_t t, id, size;
        auto extra_features = vector<uint16_t>(n_extra_fields);
        while ((infile >> t >> id >> size) && seq != n_early_stop) {
            for (int i = 0; i < n_extra_fields; ++i)
                infile >> extra_features[i];
            count_map[id] += 1;
            seq++;
        }
    }

    void insert(SimpleRequest &req) {
        uint64_t key = req.get_id();
        uint64_t count = count_map[key];
        if (count == 0) {
            cerr << "Only objects that have been seen should be recorded. " << endl;
            exit(-1);
        }
        int index = min(count - 1, counter_buckets.size() - 1);
        counter_buckets[index] += 1;
    }

    void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) {
        doc.append(kvp("access_frequency_buckets", [this](sub_array child) {
            for (const auto &element : counter_buckets)
                child.append(element);
        }));
    }
};


class AccessResourceCounter {
public:
    unordered_map <uint64_t, uint64_t> count_map;
    unordered_map <uint64_t, uint64_t> size_map;
    unordered_map <uint64_t, uint64_t> seq_map;

    vector <int64_t> counter_buckets;
    int bucket_count;
    int seq = 0;
    AccessResourceCounter(
            const string &trace_file, uint n_extra_fields,
            int64_t n_early_stop = -1, int _bucket_count = 4) {
        cerr << "init AccessResourceCounter" << endl;
        bucket_count = _bucket_count;

        for (int i = 0; i < bucket_count; i++) {
            counter_buckets.push_back(0);
        }

        // count object count until n_early_stop
        ifstream infile(trace_file);
        if (!infile) {
            cerr << "Exception opening/reading file " << trace_file << endl;
            exit(-1);
        }
        uint64_t t, id, size;
        auto extra_features = vector<uint16_t>(n_extra_fields);
        while ((infile >> t >> id >> size) && seq != n_early_stop) {
            for (int i = 0; i < n_extra_fields; ++i)
                infile >> extra_features[i];
            count_map[id] += 1;
            seq++;
        }
        seq = 0;
    }

    void incr_seq() {
        seq++;
    }

    void on_admit(SimpleRequest &req) {
        uint64_t key = req.get_id();
        size_map[key] = req.get_size();
        seq_map[key] = req.get_t();
    }

    void on_evict(uint64_t key) {
//        cerr << "AccessResourceCounter on_evict" << " " << key << endl;
        add_resource(key);
        size_map.erase(key);
        seq_map.erase(key);
    }

    void add_resource(uint64_t key) {
        uint64_t count = count_map[key];
        int index = min(count - 1, counter_buckets.size() - 1);
        auto resource = size_map[key] * (seq - seq_map[key]);
        counter_buckets[index] += resource;
    }

    void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) {
        for (const auto &pair : size_map ) {
            add_resource(pair.first);
        }
        size_map.clear();
        seq_map.clear();

        doc.append(kvp("access_resource_buckets", [this](sub_array child) {
            for (const auto &element : counter_buckets)
                child.append(element);
        }));
    }
};


#endif //LRB_BLOOM_H
