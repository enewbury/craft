#include <erl_nif.h>
#include <time.h>

static ERL_NIF_TERM clock_monotonic(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    return enif_make_atom(env, "error");
  }

  ERL_NIF_TERM sec = enif_make_int64(env, ts.tv_sec);
  ERL_NIF_TERM nsec = enif_make_int64(env, ts.tv_nsec);

  return enif_make_tuple2(env, sec, nsec);
}

/* CLOCK_MONOTONIC_COARSE isn't available on macos */
static ERL_NIF_TERM clock_monotonic_coarse(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  #ifdef CLOCK_MONOTONIC_COARSE
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC_COARSE, &ts) != 0) {
      return enif_make_atom(env, "error");
    }

    ERL_NIF_TERM sec = enif_make_int64(env, ts.tv_sec);
    ERL_NIF_TERM nsec = enif_make_int64(env, ts.tv_nsec);

    return enif_make_tuple2(env, sec, nsec);
  #else
    return enif_make_atom(env, "error");
  #endif
}

static ERL_NIF_TERM clock_monotonic_raw(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC_RAW, &ts) != 0) {
    return enif_make_atom(env, "error");
  }

  ERL_NIF_TERM sec = enif_make_int64(env, ts.tv_sec);
  ERL_NIF_TERM nsec = enif_make_int64(env, ts.tv_nsec);

  return enif_make_tuple2(env, sec, nsec);
}

static ErlNifFunc nif_funcs[] = {
  {"clock_monotonic", 0, clock_monotonic},
  {"clock_monotonic_coarse", 0, clock_monotonic_coarse},
  {"clock_monotonic_raw", 0, clock_monotonic_raw},
};

ERL_NIF_INIT(Elixir.Craft.GlobalTimestamp.NativeClock, nif_funcs, NULL, NULL, NULL, NULL)
