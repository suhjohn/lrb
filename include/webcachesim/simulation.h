//
// Created by Zhenyu Song on 10/30/18.
//

#ifndef WEBCACHESIM_SIMULATION_H
#define WEBCACHESIM_SIMULATION_H

#include <string>
#include <chrono>
#include <fstream>
#include <vector>
#include <bsoncxx/builder/basic/document.hpp>
#include <unordered_set>
#include "cache.h"
#include "filter.h"
#include "bsoncxx/document/view.hpp"
#include "bloom_filter.h"
#include "filters/bloom.h"

/*
 * single thread simulation. Returns results.
 */
bsoncxx::builder::basic::document simulation(std::string trace_file, std::string cache_type,
                                             uint64_t cache_size, std::map<std::string, std::string> params);

using namespace webcachesim;

class FrameWork {
public:
    bool uni_size = false;
    uint64_t segment_window = 1000000;
    //unit: second
    uint64_t real_time_segment_window = 600;
    uint n_extra_fields = 0;
    bool is_metadata_in_cache_size = true;
    unique_ptr<Cache> webcache = nullptr;
    unique_ptr <Filter> filter = nullptr;
    uint64_t filter_bytes_used = 0;
    int version = 2;
    std::ifstream infile;
    int64_t n_early_stop = -1;  //-1: no stop
    int64_t seq_start = 0;

    std::string _trace_file;
    std::string _cache_type;
    uint64_t _cache_size;
    const unordered_set<string> offline_algorithms = {"Belady", "BeladySample", "RelaxedBelady", "BinaryRelaxedBelady",
                                                      "PercentRelaxedBelady"};
    bool is_offline;

    /*
     * bloom filter
     */
    bool bloom_filter = false;
//    AkamaiBloomFilter *filter;

    //=================================================================
    //simulation parameter
    uint64_t t, id, size, next_seq;
    //measure every segment
    uint64_t byte_req = 0, byte_miss = 0, obj_req = 0, obj_miss = 0;
    //rt: real_time
    uint64_t rt_byte_req = 0, rt_byte_miss = 0, rt_obj_req = 0, rt_obj_miss = 0;
    //global statistics
    std::vector<int64_t> seg_byte_req, seg_byte_miss, seg_object_req, seg_object_miss;
    std::vector<int64_t> seg_rss;
    std::vector<int64_t> seg_byte_in_cache;
    //rt: real_time
    std::vector<int64_t> rt_seg_byte_req, rt_seg_byte_miss, rt_seg_object_req, rt_seg_object_miss;
    std::vector<int64_t> rt_seg_rss;
    uint64_t time_window_end;
    std::vector<uint16_t> extra_features;
    //use relative seq starting from 0
    uint64_t seq = 0;
    std::chrono::system_clock::time_point t_now;
    //decompose miss
    int64_t byte_miss_cache = 0;
    int64_t byte_miss_filter = 0;
    // When does the cache get filled?
    int64_t cache_filled_seq = -1;

    // Parameter for counting k-th hit objects that are not in cache
    bool bloom_track_k_hit = false;
    KHitCounter *kHitCounter;

    bool bloom_track_fp = false;
    FPCounter *fpCounter;

    bool track_cache_hit = false;
    ofstream cache_hit_ofstream;

    bool track_access_frequency_hit = false;
    AccessFrequencyCounter *accessFrequencyCounter;

    bool track_access_resource_hit = false;
    AccessResourceCounter *accessResourceCounter;

    bool track_eviction_age = false;
    EvictionAgeMeanTracker *evictionAgeMeanTracker;

    bool track_no_hit_eviction_age = false;
    EvictionAgeNoHitMeanTracker * evictionAgeNoHitMeanTracker;

    bool track_access_age_hit = false;
    AccessAgeCounter *accessAgeCounter;

    bool track_admission = false; // track the stats of admissions
    AdmitTracker *admitTracker;

    bool track_interrequest_classification = false;
    TraceInterrequestClassification *traceInterrequestClassification;

    FrameWork(const std::string &trace_file, const std::string &cache_type, const uint64_t &cache_size,
              std::map<std::string, std::string> &params);

    bsoncxx::builder::basic::document simulate();

    bsoncxx::builder::basic::document simulation_results();

    void adjust_real_time_offset();

    void update_real_time_stats();

    void update_stats();
};



#endif //WEBCACHESIM_SIMULATION_H
