CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -pthread

TARGET = kafka_broker
SRC = kafka_broker.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC)

clean:
	rm -f $(TARGET)

.PHONY: all clean