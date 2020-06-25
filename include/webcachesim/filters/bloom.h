//
// Created by John Suh on 5/16/20.
//

#ifndef LRB_BLOOM_H
#define LRB_BLOOM_H


#include <fstream>
#include <algorithm>
#include <vector>
#include <cmath>

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
            } else if (it.first == "bloom_record_reset") {
                int _val = stoi(it.second);
                record_reset = _val != 0;
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
        if (record_reset) {
            doc.append(kvp("refresh_indices", [this](sub_array child) {
                for (const auto &element : refresh_indices)
                    child.append(element);
            }));
        }
    }

    bool should_filter(SimpleRequest &req) override;

    size_t max_n_element = 40000000;
    double fp_rate = 0.001;
    uint16_t curr_filter_idx = 0;
    int n_added_obj = 0;
    int k = 2;
    bool record_reset = false;
    std::vector <int64_t> refresh_indices;
    std::vector<bf::basic_bloom_filter *> filters;
};

static FilterFactory <BloomFilter> factoryBloomFilter("Bloom");


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
            } else if (it.first == "bloom_record_reset") {
                int _val = stoi(it.second);
                record_reset = _val != 0;
            } else {
                cerr << "Set filter unrecognized parameter: " << it.first << endl;
            }
        }
        cerr << "Init Set filter. max_n_element: " << max_n_element << " k: " << k << endl;
        for (int i = 0; i < k; i++) {
            unordered_set <uint64_t> filter;
            filters.push_back(filter);
        }

        BloomFilter *bloom_filter = new BloomFilter();
        bloom_filter->init_with_params(params);
        _total_bytes_used = bloom_filter->total_bytes_used();
        delete bloom_filter;
    }

    size_t total_bytes_used() override {
        return _total_bytes_used;
    }

    void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) override {
        doc.append(kvp("filter_size", std::to_string(total_bytes_used())));
        doc.append(kvp("max_n_element", std::to_string(max_n_element)));
        doc.append(kvp("filter_k", k));
        if (record_reset) {
            doc.append(kvp("refresh_indices", [this](sub_array child) {
                for (const auto &element : refresh_indices)
                    child.append(element);
            }));
        }
    }

    bool should_filter(SimpleRequest &req) override;

    size_t _total_bytes_used = 0;
    size_t max_n_element = 40000000;
    uint16_t curr_filter_idx = 0;
    int n_added_obj = 0;
    int k = 2;
    bool record_reset = false;
    std::vector <int64_t> refresh_indices;
    std::vector <unordered_set<uint64_t>> filters;
};

static FilterFactory <SetFilter> factorySetFilter("Set");

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
            int64_t n_early_stop = -1, uint64_t _segment_window = 10000000,
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
    vector<double> current_buckets;
    vector <vector<double>> counter_buckets;

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
        counter_buckets.push_back(vector<double>(current_buckets));
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
        double size = size_map[key];
        double eviction_age = seq - seq_map[key];
        auto resource = size * eviction_age / reduction_factor;
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

class EvictionAgeMeanTracker {

public:
    unordered_map <uint64_t, uint64_t> seq_map;
    vector <float> mean_eviction_age_arr;

    int64_t segment_total_eviction_age;
    int64_t segment_window = 1000000;
    int64_t segment_eviction_count; // counts # of objects evicted
    uint64_t seq; // counts # of objects seen

    EvictionAgeMeanTracker() {
        segment_total_eviction_age = 0;
        segment_eviction_count = 0;
        seq = 0;
    }

    void on_admit(SimpleRequest &req) {
        uint64_t key = req.get_id();
        seq_map[key] = req.get_t();
    }

    void record_to_bucket(){
        if (segment_eviction_count > 0){
            mean_eviction_age_arr.push_back(
                (float) segment_total_eviction_age / segment_eviction_count);
        } else {
            mean_eviction_age_arr.push_back(0);
        };
        segment_total_eviction_age = 0;
        segment_eviction_count = 0;
    };

    void incr_seq() {
        if (seq && !(seq % segment_window)) {
            record_to_bucket();
        }
        seq++;
    }

    void on_evict(uint64_t key) {
        auto diff = seq - seq_map[key];
        segment_total_eviction_age += diff;
        segment_eviction_count++;
        seq_map.erase(key);
    }

    void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) {
        record_to_bucket();

        doc.append(kvp("mean_eviction_age_arr", [this](sub_array child) {
            for (const auto &element : mean_eviction_age_arr)
                child.append(element);
        }));
        doc.append(kvp("mean_eviction_age_arr_segment_window", segment_window));
    }
};


class EvictionAgeNoHitMeanTracker {

public:
    unordered_map <uint64_t, uint64_t> seq_map;
    vector <float> mean_eviction_age_arr;
    unordered_set <uint64_t> hit_map;
    int64_t segment_total_eviction_age;
    int64_t segment_window = 1000000;
    int64_t segment_eviction_count; // counts # of objects evicted
    uint64_t seq; // counts # of objects seen

    EvictionAgeNoHitMeanTracker() {
        segment_total_eviction_age = 0;
        segment_eviction_count = 0;
        seq = 0;
    }

    void on_admit(SimpleRequest &req) {
        uint64_t key = req.get_id();
        seq_map[key] = req.get_t();
    }

    void on_hit(SimpleRequest &req) {
        uint64_t key = req.get_id();
        hit_map.insert(key);
    }

    void record_to_bucket(){
        if (segment_eviction_count > 0){
            mean_eviction_age_arr.push_back(
                    (float) segment_total_eviction_age / segment_eviction_count);
        } else {
            mean_eviction_age_arr.push_back(0);
        };
        segment_total_eviction_age = 0;
        segment_eviction_count = 0;
    };

    void incr_seq() {
        if (seq && !(seq % segment_window)) {
            record_to_bucket();
        }
        seq++;
    }

    void on_evict(uint64_t key) {
        auto diff = seq - seq_map[key];
        if (hit_map.find(key) == hit_map.end()){
            segment_total_eviction_age += diff;
            segment_eviction_count++;
        }
        hit_map.erase(key);
        seq_map.erase(key);
    }

    void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) {
        record_to_bucket();

        doc.append(kvp("mean_eviction_age_arr", [this](sub_array child) {
            for (const auto &element : mean_eviction_age_arr)
                child.append(element);
        }));
        doc.append(kvp("mean_eviction_age_arr_segment_window", segment_window));
    }

};



class AccessAgeCounter {
public:
    unordered_map<int, int> seq_age_map;
    unordered_map<uint64_t, int> count_map;

    vector <vector<int64_t>> obj_freq_buckets;
    vector <vector<vector < int64_t>>>
    obj_freq_buckets_list;

    vector <vector<int64_t>> req_freq_buckets;
    vector <vector<vector < int64_t>>>
    req_freq_buckets_list;

    CountingSetFilter *filter;

    int bucket_count;
    int freq_bucket_count;
    uint64_t segment_window;
    int seq;
    int bucket_detail = 1;

    AccessAgeCounter(
            const string &trace_file, uint n_extra_fields,
            int64_t n_early_stop = -1, uint64_t _segment_window = 1000000,
            int _bucket_count = 35, int _freq_bucket_count = 3) {
        cerr << "init AccessAgeCounter" << endl;
        bucket_count = _bucket_count * bucket_detail; // More fine grained buckets
        freq_bucket_count = _freq_bucket_count;
        segment_window = _segment_window;

        for (int i = 0; i < bucket_count; i++) {
            vector <int64_t> obj_freq_bucket_base;
            vector <int64_t> req_freq_bucket_base;

            for (int j = 0; j < _freq_bucket_count; j++) {
                obj_freq_bucket_base.push_back(0);
                req_freq_bucket_base.push_back(0);
            }
            obj_freq_buckets.push_back(obj_freq_bucket_base);
            req_freq_buckets.push_back(req_freq_bucket_base);
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
        seq = 1; // we don't want 0 to be a key so we just set it as 1
        while ((infile >> t >> id >> size) && seq != n_early_stop) {
            for (int i = 0; i < n_extra_fields; ++i)
                infile >> extra_features[i];
            if (obj_prev_seq[id]) {
                seq_age_map[seq] = seq - obj_prev_seq[id];
            };
            count_map[id] += 1;
            obj_prev_seq[id] = seq;
            seq++;
        }
        seq = 1;
    }

    void init_req_counter(const std::map <std::string, std::string> &params) {
        filter = new CountingSetFilter();
        filter->init_with_params(params);
    }

    void incr_req_count(SimpleRequest &req) {
        auto size = req.get_size();
        filter->should_filter(req);
    }

    void reset_buckets() {
        for (int i = 0; i < bucket_count; i++) {
            for (int j = 0; j < freq_bucket_count; j++) {
                obj_freq_buckets[i][j] = 0;
                req_freq_buckets[i][j] = 0;
            }
        }
    }

    void record_buckets() {
        obj_freq_buckets_list.push_back(
                vector < vector < int64_t >> (obj_freq_buckets)
        );
        req_freq_buckets_list.push_back(
                vector < vector < int64_t >> (req_freq_buckets)
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
        uint64_t seq = req.get_t() + 1;
        uint64_t key = req.get_id();
        int age = seq_age_map[seq];
        int obj_count = count_map[key];
        int req_count = filter->count(req);

        // get bits of age
//        uint64_t bits, var = (age < 0) ? -age : age;
//        for (bits = 0; var != 0; ++bits) var >>= 1;
        float bucket_base = log2(age);
        size_t bucket = bucket_base * bucket_detail;
        int i = min(bucket, obj_freq_buckets.size() - 1);

        int obj_j = min(obj_count - 1, freq_bucket_count - 1);
        int req_j = min(req_count - 1, freq_bucket_count - 1);
        obj_freq_buckets[i][obj_j] += req.get_size();
        req_freq_buckets[i][req_j] += req.get_size();
    }

    void update_stat(bsoncxx::v_noabi::builder::basic::document &doc) {
        record_buckets();

        auto bytes_classified_by_obj_freq = bsoncxx::builder::stream::array{};
        auto bytes_classified_by_req_freq = bsoncxx::builder::stream::array{};

        for (int i = 0; i < obj_freq_buckets_list.size(); i++) {
            bytes_classified_by_obj_freq << open_array;
            for (int j = 0; j < bucket_count; j++) {
                bytes_classified_by_obj_freq << open_array;
                for (int k = 0; k < freq_bucket_count; k++) {
                    bytes_classified_by_obj_freq << obj_freq_buckets_list[i][j][k];
                }
                bytes_classified_by_obj_freq << close_array;
            }
            bytes_classified_by_obj_freq << close_array;
        }


        for (int i = 0; i < req_freq_buckets_list.size(); i++) {
            bytes_classified_by_req_freq << open_array;
            for (int j = 0; j < bucket_count; j++) {
                bytes_classified_by_req_freq << open_array;
                for (int k = 0; k < freq_bucket_count; k++) {
                    bytes_classified_by_req_freq << obj_freq_buckets_list[i][j][k];
                }
                bytes_classified_by_req_freq << close_array;
            }
            bytes_classified_by_req_freq << close_array;
        }

        doc.append(kvp("access_age_buckets_obj_freq_bytes", bytes_classified_by_obj_freq));
        doc.append(kvp("access_age_buckets_req_freq_bytes", bytes_classified_by_req_freq));
    }
};

#endif //LRB_BLOOM_H
