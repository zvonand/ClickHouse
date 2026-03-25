#include <Common/thread_local_rng.h>
#include <Common/randomSeed.h>

// Trigger coverage collection.
thread_local pcg64 thread_local_rng{randomSeed()};
