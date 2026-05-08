CXX := g++
SRC := src/main.cc
BIN := rht
OUTPUT_DIR := build
# We tell the compiler were to find gcc runtime libraries
LDFLAGS += -Wl,-rpath,/opt/gcc-13.4.0/lib64 -pthread 
CXXFLAGS := -std=c++20 -Wall -Wextra -Iinclude #-DDUMP 

ifeq ($(DEBUG),1)
  CXXFLAGS += -O0 -g -DLOG_LEVEL=DEBUG
else
  CXXFLAGS += -O3 -DLOG_LEVEL=RELEASE
endif

all: $(BIN)

$(BIN): $(SRC)
	$(CXX) $(CXXFLAGS) $< -o $(OUTPUT_DIR)/$@ $(LDFLAGS)

clean:
	rm -f $(OUTPUT_DIR)/*
