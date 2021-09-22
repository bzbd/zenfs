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

int test_snapshot_zone_allocation() {
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

  auto meta_snapshot_zones = zbd->GetMetaSnapshotZones();
  
#ifdef WITH_ZENFS_ASYNC_METAZONE_ROLLOVER
  const int zenfs_snapshot_zones_num = 2;
  if (meta_snapshot_zones.size() != zenfs_snapshot_zones_num) {
    fprintf(stderr, "Error! Failed to allocate meta snapshot zone");
    return 1;
  }
#else
  if (meta_snapshot_zones.size() == 0) {
    std::cout << "Success! Did not allocate meta snapshot zone\n";
    return 0;
  }
#endif

  return 0;
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char **argv) {
  gflags::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                          +" <command> [OPTIONS]...\nCommands: mkfs, list, "
                           "ls-uuid, df, backup, restore");

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  ROCKSDB_NAMESPACE::test_metadata_rollover();
  ROCKSDB_NAMESPACE::test_snapshot_zone_allocation();

  return 0;
}
