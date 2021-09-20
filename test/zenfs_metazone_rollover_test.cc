#include "utils.h"

namespace ROCKSDB_NAMESPACE {

int test_metadata_rollover() {
  std::shared_ptr<Logger> logger;
  Status s;

  s = Env::Default()->NewLogger(GetLogFilename(FLAGS_zbd), &logger);
  if (!s.ok()) {
    fprintf(stderr, "ZenFS: Could not create logger");
  } else {
    logger->SetInfoLogLevel(DEBUG_LEVEL);
  }

  ZonedBlockDevice *zbd = zbd_open(false, logger);
  if (zbd == nullptr) return 1;

  ZenFS *zenFS;
  s = zenfs_mount(zbd, &zenFS, false, logger);
  if (!s.ok()) {
#ifdef WITH_ZENFS_ASYNC_METAZONE_ROLLOVER
    assert(find_sub_string(s.ToString().c_str(), "Calling experimental function "
                                                 "ZenFS::RollMetaZoneAsync()"));
    return 0;
#endif
    fprintf(stderr, "Failed to mount filesystem, error: %s\n", s.ToString().c_str());
    return 1;
  }

  return 0;
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char **argv) {
  gflags::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                          +" <command> [OPTIONS]...\nCommands: mkfs, list, "
                           "ls-uuid, df, backup, restore");

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  return ROCKSDB_NAMESPACE::test_metadata_rollover();
}
