#pragma once

#include <cstdint> 

#include "util.h"

class Paxos {
  public:
  explicit Paxos(uint64_t system_size) : system_size_(system_size) {}

  bool Propose(){
    return false;
  }
  bool Prepare(){

  }
  bool Accept(){

  }
  private:
  uint64_t system_size_;
};