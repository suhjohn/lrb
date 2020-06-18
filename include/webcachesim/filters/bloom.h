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
#include "bsoncxx/builder/stream/document.hpp"
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include "bsoncxx/json.hpp"

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::sub_array;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::open_array;
using bsoncxx::builder::stream::close_array;
using bsoncxx::builder::stream::open_document;
using bsoncxx::builder::stream::close_document;

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
    vector <int64_t> current_buckets;
    vector <vector<int64_t>> counter_buckets;


    int bucket_count;
    uint64_t segment_window;
    int seq = 0;

    AccessFrequencyCounter(
            const string &trace_file, uint n_extra_fields,
            int64_t n_early_stop = -1, uint64_t _segment_window = 1000000,
            int _bucket_count = 4) {
        cerr << "init AccessFrequencyCounter" << endl;
        bucket_count = _bucket_count;
        segment_window = _segment_window;

        for (int i = 0; i < bucket_count; i++) {
            current_buckets.push_back(0);
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

    void reset_buckets() {
        for (int i = 0; i < bucket_count; i++) {
            current_buckets[i] = 0;
        }
    }

    void record_buckets() {
        counter_buckets.push_back(vector<int64_t>(current_buckets));
        reset_buckets();
    }

    void incr_seq() {
        if (seq && !(seq % segment_window)) {
            record_buckets();
        }
        seq++;
    }

    void insert(SimpleRequest &req) {
        uint64_t key = req.get_id();
        uint64_t count = count_map[key];
        int index = min(count - 1, current_buckets.size() - 1);
        current_buckets[index] += 1;
    }

    void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) {
        record_buckets();

        auto arr = bsoncxx::builder::stream::array{};
        for (int i = 0; i < counter_buckets.size(); i++) {
            arr << open_array;
            for (int j = 0; j < counter_buckets[i].size(); j++) {
                arr << counter_buckets[i][j];
            }
            arr << close_array;
        }

        doc.append(kvp("access_frequency_buckets", arr));
    }
};


class AccessResourceCounter {
public:
    unordered_map <uint64_t, uint64_t> count_map;
    unordered_map <uint64_t, uint64_t> size_map;
    unordered_map <uint64_t, uint64_t> seq_map;
    vector <int64_t> current_buckets;
    vector <vector<int64_t>> counter_buckets;

    int bucket_count;
    uint64_t segment_window;
    int seq = 0;
    int reduction_factor;

    AccessResourceCounter(
            const string &trace_file, uint n_extra_fields,
            int64_t n_early_stop = -1, uint64_t _segment_window = 1000000,
            int _reduction_factor = 1, int _bucket_count = 4) {
        cerr << "init AccessResourceCounter" << endl;
        bucket_count = _bucket_count;
        segment_window = _segment_window;
        reduction_factor = _reduction_factor;

        for (int i = 0; i < bucket_count; i++) {
            current_buckets.push_back(0);
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

    void reset_buckets() {
        for (int i = 0; i < bucket_count; i++) {
            current_buckets[i] = 0;
        }
    }

    void record_buckets() {
        counter_buckets.push_back(vector<int64_t>(current_buckets));
        reset_buckets();
    }

    void incr_seq() {
        if (seq && !(seq % segment_window)) {
            record_buckets();
        }
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
        int index = min(count - 1, current_buckets.size() - 1);
        auto resource = size_map[key] * (seq - seq_map[key]) / reduction_factor;
        current_buckets[index] += resource;
    }

    void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) {
        for (const auto &pair : size_map) {
            add_resource(pair.first);
        }
        record_buckets();
        size_map.clear();
        seq_map.clear();

        auto arr = bsoncxx::builder::stream::array{};
        for (int i = 0; i < counter_buckets.size(); i++) {
            arr << open_array;
            for (int j = 0; j < counter_buckets[i].size(); j++) {
                arr << counter_buckets[i][j];
            }
            arr << close_array;
        }

        doc.append(kvp("access_resource_buckets", arr));
        doc.append(kvp("access_resource_counter_reduction_factor", reduction_factor));
    }
};


class AccessAgeCounter {
public:
    unordered_map<int, int> seq_age_map;
    vector <int64_t> current_buckets;
    vector <vector<int64_t>> counter_buckets;

    vector <int64_t> current_buckets_bytes;
    vector <vector<int64_t>> counter_buckets_bytes;

    int bucket_count;
    uint64_t segment_window;
    int seq = 1;

    AccessAgeCounter(
            const string &trace_file, uint n_extra_fields,
            int64_t n_early_stop = -1, uint64_t _segment_window = 1000000,
            int _bucket_count = 40) {
        cerr << "init AccessAgeCounter" << endl;
        bucket_count = _bucket_count;
        segment_window = _segment_window;

        for (int i = 0; i < bucket_count; i++) {
            current_buckets.push_back(0);
            current_buckets_bytes.push_back(0);
        }

        // count object count until n_early_stop
        ifstream infile(trace_file);
        if (!infile) {
            cerr << "Exception opening/reading file " << trace_file << endl;
            exit(-1);
        }
        uint64_t t, id, size;
        unordered_map<int, int> obj_prev_seq;
        auto extra_features = vector<uint16_t>(n_extra_fields);
        while ((infile >> t >> id >> size) && seq != n_early_stop) {
            for (int i = 0; i < n_extra_fields; ++i)
                infile >> extra_features[i];
            if (obj_prev_seq[id]) {
                seq_age_map[seq] = seq - obj_prev_seq[id];
            };
            obj_prev_seq[id] = seq;
            seq++;
        }
        seq = 1;
    }

    void reset_buckets() {
        for (int i = 0; i < bucket_count; i++) {
            current_buckets[i] = 0;
            current_buckets_bytes[i] = 0;
        }
    }

    void record_buckets() {
        counter_buckets.push_back(vector<int64_t>(current_buckets));
        counter_buckets_bytes.push_back(
                vector<int64_t>(current_buckets_bytes)
        );
        reset_buckets();
    }

    void incr_seq() {
        if (seq && !(seq % segment_window)) {
            record_buckets();
        }
        seq++;
    }

    void insert(SimpleRequest &req) {
        uint64_t key = req.get_t() + 1;
        int age = seq_age_map[key];
        cerr << seq << " " << age << endl;

        // get bits of age
        uint64_t bits, var = (age < 0) ? -age : age;
        for (bits = 0; var != 0; ++bits) var >>= 1;

        int index = min(bits, current_buckets.size() - 1);
        current_buckets[index] += 1;
        current_buckets_bytes[index] += req.get_size();
    }

    void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) {
        record_buckets();

        auto arr = bsoncxx::builder::stream::array{};
        for (int i = 0; i < counter_buckets.size(); i++) {
            arr << open_array;
            for (int j = 0; j < counter_buckets[i].size(); j++) {
                arr << counter_buckets[i][j];
            }
            arr << close_array;
        }

        auto arr_bytes = bsoncxx::builder::stream::array{};
        for (int i = 0; i < counter_buckets_bytes.size(); i++) {
            arr_bytes << open_array;
            for (int j = 0; j < counter_buckets_bytes[i].size(); j++) {
                arr_bytes << counter_buckets_bytes[i][j];
            }
            arr_bytes << close_array;
        }

        doc.append(kvp("access_age_buckets", arr));
        doc.append(kvp("access_age_buckets_bytes", arr_bytes));
    }
};

#endif //LRB_BLOOM_H
