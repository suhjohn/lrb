//
// Created by zhenyus on 1/16/19.
//

#include "gdbt.h"
#include <algorithm>
#include "utils.h"
#include <chrono>

using namespace chrono;
using namespace std;

void GDBTCache::train() {
    auto timeBegin = chrono::system_clock::now();
    if (booster)
        LGBM_BoosterFree(booster);
    // create training dataset
    DatasetHandle trainData;
    LGBM_DatasetCreateFromCSR(
            static_cast<void *>(training_data.indptr.data()),
            C_API_DTYPE_INT32,
            training_data.indices.data(),
            static_cast<void *>(training_data.data.data()),
            C_API_DTYPE_FLOAT64,
            training_data.indptr.size(),
            training_data.data.size(),
            n_feature,  //remove future t
            GDBT_train_params,
            nullptr,
            &trainData);

    LGBM_DatasetSetField(trainData,
                         "label",
                         static_cast<void *>(training_data.labels.data()),
                         training_data.labels.size(),
                         C_API_DTYPE_FLOAT32);

    // init booster
    LGBM_BoosterCreate(trainData, GDBT_train_params, &booster);
    // train
    for (int i = 0; i < stoi(GDBT_train_params["num_iterations"]); i++) {
        int isFinished;
        LGBM_BoosterUpdateOneIter(booster, &isFinished);
        if (isFinished) {
            break;
        }
    }

    int64_t len;
    vector<double > result(training_data.indptr.size()-1);
    LGBM_BoosterPredictForCSR(booster,
                              static_cast<void *>(training_data.indptr.data()),
                              C_API_DTYPE_INT32,
                              training_data.indices.data(),
                              static_cast<void *>(training_data.data.data()),
                              C_API_DTYPE_FLOAT64,
                              training_data.indptr.size(),
                              training_data.data.size(),
                              n_feature,  //remove future t
                              C_API_PREDICT_NORMAL,
                              0,
                              GDBT_train_params,
                              &len,
                              result.data());
    double se = 0;
    for (int i = 0; i < result.size(); ++i) {
        auto diff = result[i] - training_data.labels[i];
        se += diff * diff;
    }
    training_error = training_error * 0.99 + se/batch_size*0.01;

//    vector<double > importance(n_feature);
//    LGBM_BoosterFeatureImportance(booster,
//            0,
//            0,
//            importance.data());
//    cout<<"i";
//    for (auto & it: importance)
//        cout<<" "<<it;
//    cout<<endl;

////    cerr<<"training l2: "<<msr<<endl;
//
////    vector<double > importance(max_n_past_timestamps+1);
////    int succeed = LGBM_BoosterFeatureImportance(booster,
////                                                0,
////                                                1,
////                                                importance.data());
////    cerr<<"\nimportance:\n";
////    for (auto & it: importance) {
////        cerr<<it<<endl;
////    }
////    char *json_model = new char[10000000];
////    int64_t out_len;
////    LGBM_BoosterSaveModelToString(booster,
////            0,
////            -1,
////            10000000,
////            &out_len,
////            json_model);
////    cout<<json_model<<endl;
////
////    LGBM_BoosterDumpModel(booster,
////                          0,
////                          -1,
////                          10000000,
////                          &out_len,
////                          json_model);
////    cout<<json_model<<endl;


    LGBM_DatasetFree(trainData);
    cerr << "Training time: "
               << chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now() - timeBegin).count()
               << " ms"
               << endl;
}

void GDBTCache::sample(uint64_t &t) {
    // warmup not finish
    if (meta_holder[0].empty() || meta_holder[1].empty())
        return;
#ifdef LOG_SAMPLE_RATE
    bool log_flag = ((double) rand() / (RAND_MAX)) < LOG_SAMPLE_RATE;
#endif
    auto n_l0 = static_cast<uint32_t>(meta_holder[0].size());
    auto n_l1 = static_cast<uint32_t>(meta_holder[1].size());
    auto rand_idx = _distribution(_generator);
    // at least sample 1 from the list, at most size of the list
    auto n_sample_l0 = min(max(uint32_t (sample_rate*n_l0/(n_l0+n_l1)), (uint32_t) 1), n_l0);
    auto n_sample_l1 = min(max(sample_rate - n_sample_l0, (uint32_t) 1), n_l1);

    //sample list 0
    {
        for (uint32_t i = 0; i < n_sample_l0; i++) {
            uint32_t pos = (uint32_t) (i + rand_idx) % n_l0;
            auto &meta = meta_holder[0][pos];
            uint64_t last_timestamp = meta._past_timestamp;
            uint64_t forget_timestamp = last_timestamp + GDBT::forget_window;
            pending_training_data.insert({forget_timestamp, GDBTPendingTrainingData(meta, t)});
        }
    }

    //sample list 1
    {
        for (uint32_t i = 0; i < n_sample_l1; i++) {
            uint32_t pos = (uint32_t) (i + rand_idx) % n_l1;
            auto &meta = meta_holder[1][pos];
            uint64_t last_timestamp = meta._past_timestamp;
            uint64_t forget_timestamp = last_timestamp + GDBT::forget_window;
            pending_training_data.insert({forget_timestamp, GDBTPendingTrainingData(meta, t)});
        }
    }
}


bool GDBTCache::lookup(SimpleRequest &req) {
    bool ret;
    if (!(req._t%1000000)) {
        cerr << "cache size: "<<_currentSize<<"/"<<_cacheSize<<endl;
        cerr << "n_metadata: "<<key_map.size()<<endl;
        assert(key_map.size() == forget_table.size());
        cerr << "n_pending: "<< pending_training_data.size() <<endl;
        cerr << "n_training: "<<training_data.labels.size()<<endl;
        cerr << "training loss: " << training_loss << endl;
    }


    //first update the metadata: insert/update, which can trigger pending data.mature
    auto it = key_map.find(req._id);
    if (it != key_map.end()) {
        //update past timestamps
        bool & list_idx = it->second.first;
        uint32_t & pos_idx = it->second.second;
        GDBTMeta & meta = meta_holder[list_idx][pos_idx];
        assert(meta._key == req._id);
        uint64_t last_timestamp = meta._past_timestamp;
        uint64_t forget_timestamp = last_timestamp + GDBT::forget_window;
        //if the key in key_map, it must also in forget table
        auto forget_it = forget_table.find(forget_timestamp);
        assert(forget_it != forget_table.end());
        //re-request
        auto pending_range = pending_training_data.equal_range(forget_timestamp);
        for (auto pending_it = pending_range.first; pending_it != pending_range.second;) {
            //mature
            float future_distance = log1p(req._t - pending_it->second.sample_time);
            //don't use label within the first forget window because the data is not static
            if (req._t > GDBT::forget_window)
                training_data.append(pending_it->second, future_distance);
            //training
            if (training_data.labels.size() == batch_size) {
                train();
                training_data.clear();
            }
            pending_it = pending_training_data.erase(pending_it);
        }
        //remove this entry
        forget_table.erase(forget_it);
        forget_table.insert({req._t+GDBT::forget_window, req._id});
        assert(key_map.size() == forget_table.size());

        //make this update after update training, otherwise the last timestamp will change
        meta.update(req._t);
        //update forget_table
        ret = !list_idx;
    } else {
        ret = false;
    }

    forget(req._t);
    //sampling
    if (!(req._t % training_sample_interval))
        sample(req._t);
    return ret;
}


void GDBTCache::forget(uint64_t &t) {
    //remove item from forget table, which is not going to be affect from update
    auto forget_it = forget_table.find(t);
    if (forget_it != forget_table.end()) {
        auto &key = forget_it->second;
        auto meta_it = key_map.find(key);
        if (!meta_it->second.first) {
            if (booster && t > GDBT::forget_window+batch_size*3)
                cerr << "warning: force evicting object passing forget window" << endl;
            auto &pos = meta_it->second.second;
            auto &meta = meta_holder[0][pos];
            assert(meta._key == key);
            key_map.erase(key);
            _currentSize -= meta._size;
            //evict
            uint32_t tail0_pos = meta_holder[0].size()-1;
            if (pos !=  tail0_pos) {
                //swap tail
                meta_holder[0][pos] = meta_holder[0][tail0_pos];
                key_map.find(meta_holder[0][tail0_pos]._key)->second.second = pos;
            }
            meta_holder[0].pop_back();
            //forget
            forget_table.erase(forget_it);
            assert(key_map.size() == forget_table.size());
        } else {
            auto &pos = meta_it->second.second;
            auto &meta = meta_holder[1][pos];
            assert(meta._key == key);
            key_map.erase(key);
            //evict
            uint32_t tail1_pos = meta_holder[1].size() - 1;
            if (pos != tail1_pos) {
                //swap tail
                meta_holder[1][pos] = meta_holder[1][tail1_pos];
                key_map.find(meta_holder[1][tail1_pos]._key)->second.second = pos;
            }
            meta_holder[1].pop_back();
            //forget
            forget_table.erase(forget_it);
            assert(key_map.size() == forget_table.size());
        }
        //timeout mature
        auto pending_range = pending_training_data.equal_range(t);
        for (auto pending_it = pending_range.first; pending_it != pending_range.second;) {
            //mature
            float future_distance = GDBT::log1p_forget_window;
            training_data.append(pending_it->second, future_distance);
            //training
            if (training_data.labels.size() == batch_size) {
                train();
                training_data.clear();
            }
            pending_it = pending_training_data.erase(pending_it);
        }
    }
}

void GDBTCache::admit(SimpleRequest &req) {
    const uint64_t & size = req._size;
    // object feasible to store?
    if (size > _cacheSize) {
        LOG("L", _cacheSize, req.get_id(), size);
        return;
    }

    auto it = key_map.find(req._id);
    if (it == key_map.end()) {
        //fresh insert
        key_map.insert({req._id, {0, (uint32_t) meta_holder[0].size()}});
        meta_holder[0].emplace_back(req._id, req._size, req._t, req._extra_features);
        _currentSize += size;
        forget_table.insert({req._t + GDBT::forget_window, req._id});
        assert(key_map.size() == forget_table.size());
        if (_currentSize <= _cacheSize)
            return;
    } else if (size + _currentSize <= _cacheSize){
        //bring list 1 to list 0
        //first move meta data, then modify hash table
        uint32_t tail0_pos = meta_holder[0].size();
        meta_holder[0].emplace_back(meta_holder[1][it->second.second]);
        uint32_t tail1_pos = meta_holder[1].size()-1;
        if (it->second.second !=  tail1_pos) {
            //swap tail
            meta_holder[1][it->second.second] = meta_holder[1][tail1_pos];
            key_map.find(meta_holder[1][tail1_pos]._key)->second.second = it->second.second;
        }
        meta_holder[1].pop_back();
        it->second = {0, tail0_pos};
        _currentSize += size;
        return;
    } else {
        //insert-evict
        auto epair = rank(req._t);
        auto & key0 = epair.first;
        auto & pos0 = epair.second;
        auto & pos1 = it->second.second;
        _currentSize = _currentSize - meta_holder[0][pos0]._size + req._size;
        swap(meta_holder[0][pos0], meta_holder[1][pos1]);
        swap(it->second, key_map.find(key0)->second);
    }
    // check more eviction needed?
    while (_currentSize > _cacheSize) {
        evict(req._t);
    }
}


pair<uint64_t, uint32_t> GDBTCache::rank(const uint64_t & t) {
    uint32_t rand_idx = _distribution(_generator) % meta_holder[0].size();
    //if not trained yet, use random
    if (booster == nullptr) {
        return {meta_holder[0][rand_idx]._key, rand_idx};
    }

    uint n_sample = min(sample_rate, (uint32_t) meta_holder[0].size());

    vector<int32_t> indptr = {0};
    vector<int32_t> indices;
    vector<double> data;
    vector<double> sizes;
    vector<uint64_t > past_timestamps;

    uint64_t counter = 0;
    for (int i = 0; i < n_sample; i++) {
        uint32_t pos = (i+rand_idx)%meta_holder[0].size();
        auto & meta = meta_holder[0][pos];
        //fill in past_interval
        indices.push_back(0);
        data.push_back(t - meta._past_timestamp);
        ++counter;
        past_timestamps.emplace_back(meta._past_timestamp);

        uint8_t j = 0;
        uint64_t this_past_timestamp = meta._past_timestamp;
        for (j = 0; j < meta._past_distance_idx && j < GDBT::max_n_past_timestamps-1; ++j) {
            uint8_t past_distance_idx = (meta._past_distance_idx - 1 - j) % GDBT::max_n_past_timestamps;
            uint64_t & past_distance = meta._past_distances[past_distance_idx];
            this_past_timestamp -= past_distance;
            if (this_past_timestamp > t - GDBT::forget_window) {
                indices.push_back(j+1);
                data.push_back(past_distance);
                ++counter;
            }
        }

        indices.push_back(GDBT::max_n_past_timestamps);
        data.push_back(meta._size);
        sizes.push_back(meta._size);
        ++counter;


        for (uint k = 0; k < GDBT::n_extra_fields; ++k) {
            indices.push_back(GDBT::max_n_past_timestamps + k + 1);
            data.push_back(meta._extra_features[k]);
        }
        counter += GDBT::n_extra_fields;

        indices.push_back(GDBT::max_n_past_timestamps+GDBT::n_extra_fields+1);
        data.push_back(j);
        ++counter;

        for (uint8_t k = 0; k < GDBT::n_edwt_feature; ++k) {
            indices.push_back(GDBT::max_n_past_timestamps + GDBT::n_extra_fields + 2 + k);
            uint32_t _distance_idx = min(uint32_t (t-meta._past_timestamp) / GDBT::edwt_windows[k],
                                         GDBT::max_hash_edwt_idx);
            data.push_back(meta._edwt[k]*GDBT::hash_edwt[_distance_idx]);
        }
        counter += GDBT::n_edwt_feature;

        //remove future t
        indptr.push_back(counter);

    }
    int64_t len;
    vector<double> result(n_sample);
    LGBM_BoosterPredictForCSR(booster,
                              static_cast<void *>(indptr.data()),
                              C_API_DTYPE_INT32,
                              indices.data(),
                              static_cast<void *>(data.data()),
                              C_API_DTYPE_FLOAT64,
                              indptr.size(),
                              data.size(),
                              n_feature,  //remove future t
                              C_API_PREDICT_NORMAL,
                              0,
                              GDBT_train_params,
                              &len,
                              result.data());
    if (objective == object_hit_rate)
        for (uint32_t i = 0; i < n_sample; ++i)
            result[i] += log1p(sizes[i]);

    double worst_score;
    uint32_t worst_pos;
    uint64_t min_past_timestamp;

    for (int i = 0; i < n_sample; ++i)
        if (!i || result[i] > worst_score || (result[i] == worst_score && (past_timestamps[i] < min_past_timestamp))) {
            worst_score = result[i];
            worst_pos = i;
            min_past_timestamp = past_timestamps[i];
        }
    worst_pos = (worst_pos+rand_idx)%meta_holder[0].size();
    auto & meta = meta_holder[0][worst_pos];
    auto & worst_key = meta._key;

    return {worst_key, worst_pos};
}

void GDBTCache::evict(const uint64_t & t) {
    auto epair = rank(t);
    uint64_t & key = epair.first;
    uint32_t & old_pos = epair.second;

    //bring list 0 to list 1
    uint32_t new_pos = meta_holder[1].size();

    meta_holder[1].emplace_back(meta_holder[0][old_pos]);
    uint32_t activate_tail_idx = meta_holder[0].size()-1;
    if (old_pos !=  activate_tail_idx) {
        //move tail
        meta_holder[0][old_pos] = meta_holder[0][activate_tail_idx];
        key_map.find(meta_holder[0][activate_tail_idx]._key)->second.second = old_pos;
    }
    meta_holder[0].pop_back();

    auto it = key_map.find(key);
    it->second.first = 1;
    it->second.second = new_pos;
    _currentSize -= meta_holder[1][new_pos]._size;
}


