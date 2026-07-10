#include <iostream>
#include <cstring>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <thread>
#include <fstream>
#include <vector>
#include <map>
#include <algorithm>
#include <sys/stat.h>

using namespace std;

// ==========================================
// VARINT HELPERS (For Compact Arrays/Strings)
// ==========================================
uint32_t read_unsigned_varint(const uint8_t*& p) {
    uint32_t value = 0;
    int shift = 0;
    while (true) {
        uint8_t b = *p++;
        value |= (b & 0x7f) << shift;
        if ((b & 0x80) == 0) break;
        shift += 7;
    }
    return value;
}

int32_t read_signed_varint(const uint8_t*& p) {
    uint32_t raw = read_unsigned_varint(p);
    return (raw >> 1) ^ -(raw & 1);
}

void write_unsigned_varint(vector<uint8_t>& buf, uint32_t value) {
    while (value >= 0x80) {
        buf.push_back((value & 0x7f) | 0x80);
        value >>= 7;
    }
    buf.push_back(value);
}

// Helper write functions for dynamic buffer building
void write_32(vector<uint8_t>& buf, uint32_t v) {
    v = htonl(v);
    uint8_t* p = (uint8_t*)&v;
    buf.insert(buf.end(), p, p + 4);
}

void write_16(vector<uint8_t>& buf, uint16_t v) {
    v = htons(v);
    uint8_t* p = (uint8_t*)&v;
    buf.insert(buf.end(), p, p + 2);
}

void write_8(vector<uint8_t>& buf, uint8_t v) {
    buf.push_back(v);
}

void write_bytes(vector<uint8_t>& buf, const uint8_t* data, size_t len) {
    buf.insert(buf.end(), data, data + len);
}

void write_64(vector<uint8_t>& buf, uint64_t v) {
    uint32_t high = htonl(v >> 32);
    uint32_t low = htonl(v & 0xFFFFFFFF);
    uint8_t* ph = (uint8_t*)&high;
    uint8_t* pl = (uint8_t*)&low;
    buf.insert(buf.end(), ph, ph + 4);
    buf.insert(buf.end(), pl, pl + 4);
}

// ==========================================
// METADATA STORAGE & KRAFT LOG PARSER
// ==========================================
struct PartitionInfo {
    int32_t partition_id;
    int32_t leader_id;
};

struct TopicInfo {
    string name;
    uint8_t uuid[16];
    vector<PartitionInfo> partitions;
};

map<string, TopicInfo> cluster_metadata;
map<string, string> uuid_to_topic_name;

void load_cluster_metadata() {
    ifstream file("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log", ios::binary);
    if (!file.is_open()) return;

    vector<uint8_t> data((istreambuf_iterator<char>(file)), istreambuf_iterator<char>());
    const uint8_t* p = data.data();
    const uint8_t* end = p + data.size();

    uuid_to_topic_name.clear();

    while (p < end) {
        if (end - p < 61) break; 
        
        p += 8; 
        uint32_t batch_length = ntohl(*(uint32_t*)p);
        p += 4;
        
        const uint8_t* batch_end = p + batch_length;
        
        p += 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4; 
        int32_t record_count = ntohl(*(int32_t*)p);
        p += 4;

        for (int i = 0; i < record_count; i++) {
            if (p >= batch_end) break;
            
            int32_t record_length = read_signed_varint(p);
            const uint8_t* record_end = p + record_length;
            
            p++; 
            read_signed_varint(p); 
            read_signed_varint(p); 
            
            int32_t key_length = read_signed_varint(p);
            if (key_length > 0) p += key_length; 
            
            int32_t value_length = read_signed_varint(p);
            if (value_length > 0) {
                uint8_t frame_version = *p++;
                uint8_t type = *p++;
                uint8_t version = *p++;
                
                if (type == 2) { 
                    int name_len = read_unsigned_varint(p) - 1; 
                    string name((char*)p, name_len);
                    p += name_len;
                    
                    uint8_t uuid[16];
                    memcpy(uuid, p, 16);
                    p += 16;
                    
                    cluster_metadata[name] = {name, {}, {}};
                    memcpy(cluster_metadata[name].uuid, uuid, 16);
                    
                    string uuid_str((char*)uuid, 16);
                    uuid_to_topic_name[uuid_str] = name;
                } 
                else if (type == 3) { 
                    int32_t part_id = ntohl(*(int32_t*)p);
                    p += 4;
                    
                    uint8_t uuid[16];
                    memcpy(uuid, p, 16);
                    p += 16;
                    
                    int replicas_count = read_unsigned_varint(p) - 1;
                    p += replicas_count * 4;
                    
                    int isr_count = read_unsigned_varint(p) - 1;
                    p += isr_count * 4;
                    
                    int rem_count = read_unsigned_varint(p) - 1;
                    p += rem_count * 4;
                    
                    int add_count = read_unsigned_varint(p) - 1;
                    p += add_count * 4;
                    
                    int32_t leader_id = ntohl(*(int32_t*)p);
                    p += 4;
                    
                    string uuid_str((char*)uuid, 16);
                    if (uuid_to_topic_name.count(uuid_str)) {
                        string name = uuid_to_topic_name[uuid_str];
                        cluster_metadata[name].partitions.push_back({part_id, leader_id});
                    }
                }
            }
            p = record_end;
        }
        p = batch_end;
    }
    
    for(auto& pair : cluster_metadata) {
        sort(pair.second.partitions.begin(), pair.second.partitions.end(), 
             [](const PartitionInfo& a, const PartitionInfo& b) {
                 return a.partition_id < b.partition_id;
             });
    }
}

void handle_client(int client_fd) {
    while (true) {
        char buffer[2048];
        ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytes_read <= 0) break; 
        if (bytes_read < 12) continue;

        uint16_t api_key = ntohs(*(uint16_t*)(buffer + 4));
        uint16_t api_version = ntohs(*(uint16_t*)(buffer + 6));
        uint32_t correlation_id = ntohl(*(uint32_t*)(buffer + 8));

        if (api_key == 18) {
            uint16_t error_code = (api_version > 4) ? 35 : 0;
            vector<uint8_t> resp;
            resp.resize(4); 
            write_32(resp, ntohl(correlation_id));
            write_16(resp, error_code);
            
            if (error_code == 0) {
                write_8(resp, 5); 
                write_16(resp, 18); write_16(resp, 0); write_16(resp, 4); write_8(resp, 0);
                write_16(resp, 75); write_16(resp, 0); write_16(resp, 0); write_8(resp, 0);
                write_16(resp, 1); write_16(resp, 0); write_16(resp, 16); write_8(resp, 0);
                write_16(resp, 0); write_16(resp, 0); write_16(resp, 11); write_8(resp, 0);
                write_32(resp, 0); write_8(resp, 0);
            }
            uint32_t total_size = htonl(resp.size() - 4);
            memcpy(resp.data(), &total_size, 4);
            send(client_fd, resp.data(), resp.size(), 0);
        } 
        else if (api_key == 1) {
            const uint8_t* req_p = (const uint8_t*)buffer + 12; 
            int16_t client_id_len = ntohs(*(int16_t*)req_p); req_p += 2 + client_id_len + 1;
            req_p += 4 + 4 + 4 + 1 + 4 + 4; 
            uint32_t num_topics = read_unsigned_varint(req_p) - 1;
            
            vector<uint8_t> resp;
            resp.resize(4); 
            write_32(resp, ntohl(correlation_id));
            write_8(resp, 0); 
            write_32(resp, 0); write_16(resp, 0); write_32(resp, 0); 
            write_unsigned_varint(resp, num_topics + 1); 
            
            for (uint32_t i = 0; i < num_topics; ++i) {
                uint8_t topic_id[16]; memcpy(topic_id, req_p, 16); req_p += 16;
                string uuid_str((char*)topic_id, 16);
                bool topic_exists = uuid_to_topic_name.count(uuid_str) > 0;
                write_bytes(resp, topic_id, 16);
                uint32_t num_partitions = read_unsigned_varint(req_p) - 1;
                write_unsigned_varint(resp, num_partitions + 1);
                for (uint32_t j = 0; j < num_partitions; ++j) {
                    int32_t partition_id = ntohl(*(int32_t*)req_p); req_p += 4 + 4 + 8 + 4 + 8 + 4 + 1;
                    write_32(resp, partition_id);
                    write_16(resp, topic_exists ? 0 : 100);
                    write_64(resp, 0); write_64(resp, 0); write_64(resp, 0);
                    write_unsigned_varint(resp, 1); write_32(resp, 0); 
                    
                    if (topic_exists) {
                        string topic_name = uuid_to_topic_name[uuid_str];
                        string file_path = "/tmp/kraft-combined-logs/" + topic_name + "-" + to_string(partition_id) + "/00000000000000000000.log";
                        ifstream part_file(file_path, ios::binary | ios::ate);
                        if (part_file.is_open()) {
                            streamsize size = part_file.tellg();
                            part_file.seekg(0, ios::beg);
                            if (size > 0) {
                                vector<uint8_t> record_bytes(size);
                                part_file.read((char*)record_bytes.data(), size);
                                write_unsigned_varint(resp, size + 1);
                                write_bytes(resp, record_bytes.data(), size);
                            } else write_8(resp, 0); 
                        } else write_8(resp, 0); 
                    } else write_8(resp, 0); 
                    write_8(resp, 0); 
                }
                req_p += 1; write_8(resp, 0); 
            }
            write_8(resp, 0); 
            uint32_t total_size = htonl(resp.size() - 4);
            memcpy(resp.data(), &total_size, 4);
            send(client_fd, resp.data(), resp.size(), 0);
        }
        else if (api_key == 0) {
            const uint8_t* req_p = (const uint8_t*)buffer + 12; 
            int16_t client_id_len = ntohs(*(int16_t*)req_p); req_p += 2 + client_id_len + 1;
            uint32_t txn_id_len = read_unsigned_varint(req_p); if(txn_id_len > 0) req_p += (txn_id_len - 1);
            req_p += 2 + 4; 
            uint32_t num_topics = read_unsigned_varint(req_p) - 1;
            
            vector<uint8_t> resp;
            resp.resize(4); 
            write_32(resp, ntohl(correlation_id));
            write_8(resp, 0); 
            write_unsigned_varint(resp, num_topics + 1);
            
            for (uint32_t i = 0; i < num_topics; ++i) {
                uint32_t name_len = read_unsigned_varint(req_p) - 1;
                string topic_name((char*)req_p, name_len); req_p += name_len;
                bool topic_exists = cluster_metadata.count(topic_name) > 0;
                write_unsigned_varint(resp, name_len + 1);
                write_bytes(resp, (const uint8_t*)topic_name.data(), name_len);
                uint32_t num_partitions = read_unsigned_varint(req_p) - 1;
                write_unsigned_varint(resp, num_partitions + 1);
                for (uint32_t j = 0; j < num_partitions; ++j) {
                    int32_t partition_id = ntohl(*(int32_t*)req_p); req_p += 4;
                    int32_t records_size = ntohl(*(int32_t*)req_p); req_p += 4;
                    const uint8_t* batch_ptr = req_p;
                    if (records_size > 0) req_p += records_size;
                    
                    bool partition_exists = false;
                    if(topic_exists) for(auto& p : cluster_metadata[topic_name].partitions) if(p.partition_id == partition_id) partition_exists = true;
                    uint16_t err = (topic_exists && partition_exists) ? 0 : 3;
                    
                    if (err == 0 && records_size > 0) {
                        string dir = "/tmp/kraft-combined-logs/" + topic_name + "-" + to_string(partition_id);
                        mkdir(dir.c_str(), 0777);
                        ofstream out(dir + "/00000000000000000000.log", ios::binary | ios::app);
                        out.write((const char*)batch_ptr, records_size);
                    }
                    write_32(resp, partition_id);
                    write_16(resp, err);
                    write_64(resp, (err == 0) ? 0 : (uint64_t)-1);
                    write_64(resp, (uint64_t)-1);
                    write_64(resp, (err == 0) ? 0 : (uint64_t)-1);
                    write_unsigned_varint(resp, 1); write_unsigned_varint(resp, 0); write_8(resp, 0);
                }
                write_8(resp, 0); 
            }
            write_32(resp, 0); write_8(resp, 0);
            uint32_t total_size = htonl(resp.size() - 4);
            memcpy(resp.data(), &total_size, 4);
            send(client_fd, resp.data(), resp.size(), 0);
        }
        else if (api_key == 75) {
            const uint8_t* req_p = (const uint8_t*)buffer + 12; 
            int16_t client_id_len = ntohs(*(int16_t*)req_p); req_p += 2 + client_id_len + 1;
            uint32_t num_topics = read_unsigned_varint(req_p) - 1;
            vector<string> requested;
            for(uint32_t i=0; i<num_topics; i++) {
                uint32_t len = read_unsigned_varint(req_p) - 1;
                requested.emplace_back((char*)req_p, len); req_p += len + 1;
            }
            sort(requested.begin(), requested.end());
            vector<uint8_t> resp; resp.resize(4);
            write_32(resp, ntohl(correlation_id));
            write_8(resp, 0); write_32(resp, 0); write_unsigned_varint(resp, requested.size() + 1);
            for(const auto& name : requested) {
                if(cluster_metadata.count(name)) {
                    write_16(resp, 0); write_unsigned_varint(resp, name.length()+1); write_bytes(resp, (uint8_t*)name.data(), name.length());
                    write_bytes(resp, cluster_metadata[name].uuid, 16); write_8(resp, 0);
                    write_unsigned_varint(resp, cluster_metadata[name].partitions.size()+1);
                    for(auto& p : cluster_metadata[name].partitions) {
                        write_16(resp, 0); write_32(resp, p.partition_id); write_32(resp, p.leader_id); write_32(resp, 0);
                        write_unsigned_varint(resp, 2); write_32(resp, p.leader_id);
                        write_unsigned_varint(resp, 2); write_32(resp, p.leader_id);
                        write_unsigned_varint(resp, 1); write_unsigned_varint(resp, 1); write_unsigned_varint(resp, 1); write_8(resp, 0);
                    }
                    write_32(resp, 0); write_8(resp, 0);
                } else {
                    write_16(resp, 3); write_unsigned_varint(resp, name.length()+1); write_bytes(resp, (uint8_t*)name.data(), name.length());
                    uint8_t z[16]={0}; write_bytes(resp, z, 16); write_8(resp, 0);
                    write_unsigned_varint(resp, 1); write_32(resp, 0); write_8(resp, 0);
                }
            }
            write_8(resp, 0xff); write_8(resp, 0);
            uint32_t total_size = htonl(resp.size() - 4); memcpy(resp.data(), &total_size, 4);
            send(client_fd, resp.data(), resp.size(), 0);
        }
    }
    close(client_fd);
}

int main() {
    load_cluster_metadata();
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1; setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in server_addr{}; server_addr.sin_family = AF_INET; server_addr.sin_addr.s_addr = INADDR_ANY; server_addr.sin_port = htons(9092);
    bind(server_fd, (sockaddr*)&server_addr, sizeof(server_addr));
    listen(server_fd, 5);
    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        thread(handle_client, client_fd).detach();
    }
    close(server_fd);
    return 0;
}
