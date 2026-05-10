CXX := g++
SRC := src/main.cc
BIN := rht
OUTPUT_DIR := build
LDFLAGS += -pthread
CXXFLAGS := -std=c++20 -Wall -Wextra -Iinclude

ifeq ($(DEBUG),1)
  CXXFLAGS += -O0 -g -DLOG_LEVEL=DEBUG
else
  CXXFLAGS += -O3 -DLOG_LEVEL=RELEASE
endif

.DEFAULT_GOAL := all
.PHONY: all debug release clean

all: $(BIN)
debug:
	$(MAKE) DEBUG=1
release:
	$(MAKE)

$(BIN): $(SRC)
	mkdir -p $(OUTPUT_DIR)
	$(CXX) $(CXXFLAGS) $< -o $(OUTPUT_DIR)/$(BIN) $(LDFLAGS)

clean:
	rm -f $(OUTPUT_DIR)/*