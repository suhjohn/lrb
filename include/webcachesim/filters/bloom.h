//
// Created by John Suh on 5/16/20.
//

#ifndef LRB_BLOOM_H
#define LRB_BLOOM_H


#include <sstream>
#include <vector>
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

//class SetFilter : public Filter {
//public:
//    SetFilter() : Filter() {}
//
//    bool should_filter(SimpleRequest &req) {
//        return !exist_or_insert(req.get_id());
//    }
//
//    inline bool exist(uint64_t key) {
//        return (_filters[0].find(key)) != _filters[0].end() || (_filters[1].find(key) != _filters[1].end());
//    }
//
//    inline bool exist_or_insert(uint64_t key) {
//        if (exist(key))
//            return true;
//        else
//            insert(key);
//        return false;
//    }
//
//    inline void insert(uint64_t key) {
//        if (n_added_obj > max_n_element) {
//            _filters[1 - current_filter].clear();
//            current_filter = 1 - current_filter;
//            n_added_obj = 0;
//        }
//        _filters[current_filter].insert(key);
//        ++n_added_obj;
//    }
//
//    void init_with_params(const std::map <std::string, std::string> &params) override {
//        for (auto &it: params) {
//            if (it.first == "max_n_element") {
//                std::istringstream iss(it.second);
//                size_t new_n;
//                iss >> new_n;
//                max_n_element = new_n;
//            } else {
//                cerr << "Set filter unrecognized parameter: " << it.first << endl;
//            }
//        }
//        cerr << "Init Set filter. max_n_element: " << max_n_element;
//        unordered_set <uint64_t> filter_1;
//        unordered_set <uint64_t> filter_2;
//        _filters[0] = filter_1;
//        _filters[1] = filter_2;
//    }
//
//    size_t max_n_element = 40000000;
//    uint8_t current_filter = 0;
//    int n_added_obj = 0;
//    unordered_set <uint64_t> _filters[2];
//};
//
//static FilterFactory <SetFilter> factorySetFilter("Set");

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
        filter->should_filter(req);
        auto should_filter = bloom_filter->should_filter(req);
        if (!should_filter) {
            if (filter->count(req) == 0) {
                false_positive += 0;
            } else {
                true_positive += 1;
            }
        }
    }
};

#endif //LRB_BLOOM_H
