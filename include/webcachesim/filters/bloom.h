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
        cerr << "Init Bloom filter. max_n_element: " << max_n_element << " k: " << k << endl;

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

    bool should_filter(SimpleRequest &req) override;

    size_t max_n_element = 40000000;
    double fp_rate = 0.001;
    uint16_t curr_filter_idx = 0;
    int n_added_obj = 0;
    int k = 2;
    bool track_kth_hit = false;

    std::vector <unordered_map<uint64_t, uint64_t>> filters;
};

static FilterFactory <CountingSetFilter> factoryCountingSetFilter("CountingSet");

class KHitCounter {
    CountingSetFilter *filter;
public:
    uint64_t second_hit_byte, unevicted_kth_hit_byte, evicted_kth_hit_byte;
    KHitCounter(const std::map <std::string, std::string> &params) {
        CountingSetFilter _filter;
        filter = &_filter;
        filter->init_with_params(params);
    }

    void insert(SimpleRequest &req) {
        filter->should_filter(req);
    }

    uint64_t count(SimpleRequest &req) {
        return filter->count(req);
    }

    uint64_t update_second_hit(uint64_t size) {
        second_hit_byte += size;
    }

    uint64_t update_unevicted_kth(uint64_t size) {
        unevicted_kth_hit_byte += size;
    }

    uint64_t update_evicted_kth(uint64_t size) {
        evicted_kth_hit_byte += size;
    }

};

#endif //LRB_BLOOM_H
