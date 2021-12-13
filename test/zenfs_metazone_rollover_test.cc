#include "utils.h"

namespace ROCKSDB_NAMESPACE {

int test_mkfs() {
  Status s;
  DIR* aux_dir;
  std::shared_ptr<Logger> logger;

  if (FLAGS_aux_path.empty()) {
      fprintf(stderr, "You need to specify --aux_path\n");
      return 1;
  }

  aux_dir = opendir(FLAGS_aux_path.c_str());
  if (ENOENT != errno) {
      fprintf(stderr, "Error: aux path exists\n");
      closedir(aux_dir);
      return 1;
  }

  s = Env::Default()->NewLogger(GetLogFilename(FLAGS_zbd), &logger);
  if (!s.ok()) {
      fprintf(stderr, "ZenFS: Could not create logger");
  } else {
      logger->SetInfoLogLevel(DEBUG_LEVEL);
  }

  ZonedBlockDevice *zbd = zbd_open(false, logger);
  if (zbd == nullptr) {
      fprintf(stderr, "Failed to open file\n");;
      return 1;
  }

  /* verify if snapshot zones being allocated properly */
  auto snapshot_zones = zbd->GetSnapshotZones();
  const int zenfs_snapshot_zones_num = 2;
  if (snapshot_zones.size() != zenfs_snapshot_zones_num) {
    fprintf(stderr, "Error! Failed to allocate snapshot zone");
    return 1;
  }

  ZenFS *zenFS;
  zenFS = new ZenFS(zbd, FileSystem::Default(), logger);

  if (FLAGS_aux_path.back() != '/') {
    FLAGS_aux_path.append("/");
  }

  s = zenFS->MkFS(FLAGS_aux_path, FLAGS_finish_threshold,
                  FLAGS_max_open_zones, FLAGS_max_active_zones);
  if (!s.ok()) {
    fprintf(stderr, "Failed to create file system, error: %s\n",
            s.ToString().c_str());
    delete zenFS;
    return 1;
  }

  /* verify if the fist snapshot zone has an empty snapshot record */
  assert(!snapshot_zones[0]->IsEmpty());
  /* verify if the reset of snapshot zones are reset properly */
  for (int i = 1; i < snapshot_zones.size(); i++) {
    assert(snapshot_zones[i]->IsEmpty());
  }

  fprintf(stdout, "ZenFS file system created. Free space: %lu MB\n",
          zbd->GetFreeSpace() / (1024 * 1024));

  delete zenFS;
  return 0;
}

int test_mount() {
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

  ROCKSDB_NAMESPACE::test_mkfs();
  ROCKSDB_NAMESPACE::test_mount();

  return 0;
}
