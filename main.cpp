#include <iostream>
#include <cstring>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

using namespace std;

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0) {
        perror("socket");
        return 1;
    }

    int reuse = 1;

    setsockopt(server_fd,
               SOL_SOCKET,
               SO_REUSEADDR,
               &reuse,
               sizeof(reuse));

    setsockopt(server_fd,
               SOL_SOCKET,
               SO_REUSEPORT,
               &reuse,
               sizeof(reuse));

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd,
             (sockaddr*)&server_addr,
             sizeof(server_addr)) < 0) {
        perror("bind");
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, 5) < 0) {
        perror("listen");
        close(server_fd);
        return 1;
    }

    cout << "Kafka broker listening on port 9092...\n";

    while (true) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        // 1. Accept a new client connection
        int client_fd =
            accept(server_fd,
                   (sockaddr*)&client_addr,
                   &client_len);

        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        // --- NEW: Inner Loop for Serial Requests ---
        // Continuously read from the same client connection until they disconnect
        while (true) {
            char buffer[1024];

            ssize_t bytes_read =
                recv(client_fd,
                     buffer,
                     sizeof(buffer),
                     0);

            // If recv returns 0, the client disconnected gracefully.
            // If recv returns < 0, a network error occurred.
            // In either case, break the inner loop to close the connection.
            if (bytes_read <= 0) {
                break; 
            }

            if (bytes_read < 12) {
                continue;
            }

            uint16_t api_version;

            memcpy(&api_version,
                   buffer + 6,
                   2);

            api_version = ntohs(api_version);

            uint16_t error_code = 0;

            if (api_version > 4)
                error_code = 35;

            // Convert the error_code back to Network Byte Order
            uint16_t error_code_network = htons(error_code);

            if (error_code == 35) {
                // --- ERROR RESPONSE (10 BYTES) ---
                char response[10];

                uint32_t message_size = htonl(6);
                memcpy(response, &message_size, 4);
                
                // Correlation ID
                memcpy(response + 4, buffer + 8, 4);
                
                // Error Code (35)
                memcpy(response + 8, &error_code_network, 2);

                ssize_t bytes_sent = send(client_fd, response, sizeof(response), 0);
                if (bytes_sent != sizeof(response)) {
                    perror("send");
                }
            } else {
                // --- SUCCESS RESPONSE (23 BYTES) ---
                char response[23];

                // 1. Message Size: 19 bytes (23 total - 4 for size field)
                uint32_t message_size = htonl(19);
                memcpy(response, &message_size, 4);

                // 2. Correlation ID: Echoed from request
                memcpy(response + 4, buffer + 8, 4);

                // 3. Error Code: 0
                memcpy(response + 8, &error_code_network, 2);

                // 4. API Keys Array Length: 1 element (+1 for COMPACT_ARRAY = 2)
                response[10] = 2;

                // 5. API Key: 18 (ApiVersions)
                uint16_t supported_api_key = htons(18);
                memcpy(response + 11, &supported_api_key, 2);

                // 6. Min Version: 0
                uint16_t min_version = htons(0);
                memcpy(response + 13, &min_version, 2);

                // 7. Max Version: 4
                uint16_t max_version = htons(4);
                memcpy(response + 15, &max_version, 2);

                // 8. TAG_BUFFER: Empty (0)
                response[17] = 0;

                // 9. Throttle Time (ms): 0
                uint32_t throttle_time_ms = htonl(0);
                memcpy(response + 18, &throttle_time_ms, 4);

                // 10. TAG_BUFFER: Empty (0)
                response[22] = 0;

                ssize_t bytes_sent = send(client_fd, response, sizeof(response), 0);
                if (bytes_sent != sizeof(response)) {
                    perror("send");
                }
            }
        } // End of inner loop

        // Connection is closed ONLY when the client disconnects and breaks the inner loop
        close(client_fd);
    }

    close(server_fd);
    return 0;
}