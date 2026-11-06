// Independent libstdc++ std::shuffle oracle; test tooling only, no production CGO.
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <numeric>
#include <vector>

struct Random {
    using result_type = uint32_t;
    uint64_t state;
    explicit Random(uint64_t seed) : state((seed + 1442695040888963407ULL) * 6364136223846793005ULL + 1442695040888963407ULL) {}
    static constexpr uint32_t min() { return 0; }
    static constexpr uint32_t max() { return UINT32_MAX; }
    uint32_t operator()() {
        uint64_t old = state;
        state = old * 6364136223846793005ULL + 1442695040888963407ULL;
        uint32_t value = uint32_t(((old >> 18) ^ old) >> 27);
        uint32_t rotation = old >> 59;
        return (value >> rotation) | (value << ((-rotation) & 31));
    }
};

int main(int argc, char** argv) {
    if (argc != 3) return 1;
    Random rng(std::strtoull(argv[1], nullptr, 10));
    std::vector<int> values(std::strtoul(argv[2], nullptr, 10));
    std::iota(values.begin(), values.end(), 0);
    std::shuffle(values.begin(), values.end(), rng);
    for (auto value : values) std::cout << value << '\n';
    std::cout << "next=" << rng() << '\n';
}
