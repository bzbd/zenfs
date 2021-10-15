// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include "fs_zenfs.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <iostream>
#include <utility>
#include <vector>

#include "rocksdb/utilities/object_registry.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "utilities/trace/bytedance_metrics_reporter.h"

#define DEFAULT_ZENV_LOG_PATH "/tmp/"

namespace ROCKSDB_NAMESPACE {

Status Superblock::DecodeFrom(Slice* input) {
  if (input->size() != ENCODED_SIZE) {
    return Status::Corruption("ZenFS Superblock",
                              "Error: Superblock size missmatch");
  }

  GetFixed32(input, &magic_);
  memcpy(&uuid_, input->data(), sizeof(uuid_));
  input->remove_prefix(sizeof(uuid_));
  GetFixed32(input, &sequence_);
  GetFixed32(input, &version_);
  GetFixed32(input, &flags_);
  GetFixed32(input, &block_size_);
  GetFixed32(input, &zone_size_);
  GetFixed32(input, &nr_zones_);
  GetFixed32(input, &finish_treshold_);
  memcpy(&aux_fs_path_, input->data(), sizeof(aux_fs_path_));
  input->remove_prefix(sizeof(aux_fs_path_));
  GetFixed32(input, &max_active_limit_);
  GetFixed32(input, &max_open_limit_);
  memcpy(&reserved_, input->data(), sizeof(reserved_));
  input->remove_prefix(sizeof(reserved_));
  assert(input->size() == 0);

  if (magic_ != MAGIC)
    return Status::Corruption("ZenFS Superblock", "Error: Magic missmatch");
  if (version_ != CURRENT_VERSION)
    return Status::Corruption("ZenFS Superblock", "Error: Version missmatch");

  return Status::OK();
}

void Superblock::EncodeTo(std::string* output) {
  sequence_++; /* Ensure that this superblock representation is unique */
  output->clear();
  PutFixed32(output, magic_);
  output->append(uuid_, sizeof(uuid_));
  PutFixed32(output, sequence_);
  PutFixed32(output, version_);
  PutFixed32(output, flags_);
  PutFixed32(output, block_size_);
  PutFixed32(output, zone_size_);
  PutFixed32(output, nr_zones_);
  PutFixed32(output, finish_treshold_);
  output->append(aux_fs_path_, sizeof(aux_fs_path_));
  PutFixed32(output, max_active_limit_);
  PutFixed32(output, max_open_limit_);
  output->append(reserved_, sizeof(reserved_));
  assert(output->length() == ENCODED_SIZE);
}

Status Superblock::CompatibleWith(ZonedBlockDevice* zbd) {
  if (block_size_ != zbd->GetBlockSize())
    return Status::Corruption("ZenFS Superblock",
                              "Error: block size missmatch");
  if (zone_size_ != (zbd->GetZoneSize() / block_size_))
    return Status::Corruption("ZenFS Superblock", "Error: zone size missmatch");
  if (nr_zones_ > zbd->GetNrZones())
    return Status::Corruption("ZenFS Superblock",
                              "Error: nr of zones missmatch");
  if (max_active_limit_ > zbd->GetMaxActiveZones())
    return Status::Corruption("ZenFS Superblock",
                              "Error: active zone limit missmatch");
  if (max_open_limit_ > zbd->GetMaxOpenZones())
    return Status::Corruption("ZenFS Superblock",
                              "Error: open zone limit missmatch");

  return Status::OK();
}

IOStatus ZenMetaLog::AddRecord(const Slice& slice) {
  uint32_t record_sz = slice.size();
  const char* data = slice.data();
  size_t phys_sz;
  uint32_t crc = 0;
  char* buffer;
  int ret;
  IOStatus s;

  phys_sz = record_sz + zMetaHeaderSize;

  if (phys_sz % bs_) phys_sz += bs_ - phys_sz % bs_;

  assert(data != nullptr);
  assert((phys_sz % bs_) == 0);

  ret = posix_memalign((void**)&buffer, sysconf(_SC_PAGESIZE), phys_sz);
  if (ret) return IOStatus::IOError("Failed to allocate memory");

  memset(buffer, 0, phys_sz);

  crc = crc32c::Extend(crc, (const char*)&record_sz, sizeof(uint32_t));
  crc = crc32c::Extend(crc, data, record_sz);
  crc = crc32c::Mask(crc);

  EncodeFixed32(buffer, crc);
  EncodeFixed32(buffer + sizeof(uint32_t), record_sz);
  memcpy(buffer + sizeof(uint32_t) * 2, data, record_sz);

  s = zone_->Append(buffer, phys_sz);

  free(buffer);
  return s;
}

IOStatus ZenMetaLog::Read(Slice* slice) {
  int f = zbd_->GetReadFD();
  const char* data = slice->data();
  size_t read = 0;
  size_t to_read = slice->size();
  int ret;

  if (read_pos_ >= zone_->wp_) {
    // EOF
    slice->clear();
    return IOStatus::OK();
  }

  if ((read_pos_ + to_read) > (zone_->start_ + zone_->max_capacity_)) {
    return IOStatus::IOError("Read across zone");
  }

  while (read < to_read) {
    ret = pread(f, (void*)(data + read), to_read - read, read_pos_);

    if (ret == -1 && errno == EINTR) continue;
    if (ret < 0) return IOStatus::IOError("Read failed");

    read += ret;
    read_pos_ += ret;
  }

  return IOStatus::OK();
}

IOStatus ZenMetaLog::ReadRecord(Slice* record, std::string* scratch) {
  Slice header;
  uint32_t record_sz = 0;
  uint32_t record_crc = 0;
  uint32_t actual_crc;
  IOStatus s;

  scratch->clear();
  record->clear();

  scratch->append(zMetaHeaderSize, 0);
  header = Slice(scratch->c_str(), zMetaHeaderSize);

  s = Read(&header);
  if (!s.ok()) return s;

  // EOF?
  if (header.size() == 0) {
    record->clear();
    return IOStatus::OK();
  }

  GetFixed32(&header, &record_crc);
  GetFixed32(&header, &record_sz);

  scratch->clear();
  scratch->append(record_sz, 0);

  *record = Slice(scratch->c_str(), record_sz);
  s = Read(record);
  if (!s.ok()) return s;

  actual_crc = crc32c::Value((const char*)&record_sz, sizeof(uint32_t));
  actual_crc = crc32c::Extend(actual_crc, record->data(), record->size());

  if (actual_crc != crc32c::Unmask(record_crc)) {
    return IOStatus::IOError("Not a valid record");
  }

  /* Next record starts on a block boundary */
  if (read_pos_ % bs_) read_pos_ += bs_ - (read_pos_ % bs_);

  return IOStatus::OK();
}

ZenFS::ZenFS(ZonedBlockDevice* zbd, std::shared_ptr<FileSystem> aux_fs,
             std::shared_ptr<Logger> logger)
    : FileSystemWrapper(aux_fs), zbd_(zbd), logger_(logger) {
  Info(logger_, "ZenFS initializing");
  Info(logger_, "ZenFS parameters: block device: %s, aux filesystem: %s",
       zbd_->GetFilename().c_str(), target()->Name());

  Info(logger_, "ZenFS initializing");
  next_file_id_ = 1;
  metadata_writer_.zenFS = this;
}

ZenFS::~ZenFS() {
  Status s;
  Info(logger_, "ZenFS shutting down");
  zbd_->LogZoneUsage();
  LogFiles();

  op_log_.reset(nullptr);
#ifdef WITH_ZENFS_ASYNC_METAZONE_ROLLOVER
  snapshot_log_.reset(nullptr);
#endif  // WITH_ZENFS_ASYNC_METAZONE_ROLLOVER
  ClearFiles();
  delete zbd_;
}

void ZenFS::LogFiles() {
  std::map<std::string, ZoneFile*>::iterator it;
  uint64_t total_size = 0;

  Info(logger_, "  Files:\n");
  for (it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zFile = it->second;
    std::vector<ZoneExtent*> extents = zFile->GetExtents();

    Info(logger_, "    %-45s sz: %lu lh: %d", it->first.c_str(),
         zFile->GetFileSize(), zFile->GetWriteLifeTimeHint());
    for (unsigned int i = 0; i < extents.size(); i++) {
      ZoneExtent* extent = extents[i];
      Info(logger_, "          Extent %u {start=0x%lx, zone=%u, len=%u} ", i,
           extent->start_,
           (uint32_t)(extent->zone_->start_ / zbd_->GetZoneSize()),
           extent->length_);

      total_size += extent->length_;
    }
  }
  Info(logger_, "Sum of all files: %lu MB of data \n",
       total_size / (1024 * 1024));
}

void ZenFS::ClearFiles() {
  std::map<std::string, ZoneFile*>::iterator it;
  files_mtx_.lock();
  for (it = files_.begin(); it != files_.end(); it++) delete it->second;
  files_.clear();
  files_mtx_.unlock();
}

/* Assumes that files_mutex_ is held */
IOStatus ZenFS::WriteSnapshotLocked(ZenMetaLog* meta_log) {
  IOStatus s;
  std::string snapshot;

  EncodeSnapshotTo(&snapshot);
  s = meta_log->AddRecord(snapshot);
  if (s.ok()) {
    for (auto it = files_.begin(); it != files_.end(); it++) {
      ZoneFile* zoneFile = it->second;
      zoneFile->MetadataSynced();
    }
  }
  return s;
}

IOStatus ZenFS::WriteEndRecord(ZenMetaLog* meta_log) {
  std::string endRecord;

  PutFixed32(&endRecord, kEndRecord);
  return meta_log->AddRecord(endRecord);
}

/* Assumes the files_mtx_ is held */
IOStatus ZenFS::RollMetaZone() {
#ifdef WITH_ZENFS_ASYNC_METAZONE_ROLLOVER
  return RollMetaZoneAsync();
#else
  return RollMetaZoneLocked();
#endif  // WITH_ZENFS_ASYNC_METAZONE_ROLLOVER
}

#ifdef WITH_ZENFS_ASYNC_METAZONE_ROLLOVER
/* Assumes the files_mtx_ is held */
IOStatus ZenFS::RollMetaZoneAsync() {
  return IOStatus::IOError(
      "Calling experimental function "
      "ZenFS::RollMetaZoneAsync()!!");
}
#endif  // WITH_ZENFS_ASYNC_METAZONE_ROLLOVER

/* Assumes the files_mtx_ is held */
IOStatus ZenFS::RollMetaZoneLocked() {
  ZenMetaLog* new_op_log;
  Zone *new_op_log_zone, *old_op_log_zone;
  IOStatus s;
  LatencyHistGuard guard(&zbd_->roll_latency_reporter_);
  zbd_->roll_qps_reporter_.AddCount(1);

  new_op_log_zone = zbd_->AllocateMetaZone();

  if (!new_op_log_zone) {
    assert(false);  // TMP
    Error(logger_, "Out of op log zones, we should go to read only now.");
    return IOStatus::NoSpace("Out of op log zones");
  }

  Info(logger_, "Rolling to op log %d\n", (int)new_op_log_zone->GetZoneNr());
  new_op_log = new ZenMetaLog(zbd_, new_op_log_zone);
  old_op_log_zone = op_log_->GetZone();
  old_op_log_zone->open_for_write_ = false;

  /* Write an end record and finish the op log data zone if there is space left
   */
  if (old_op_log_zone->GetCapacityLeft()) WriteEndRecord(op_log_.get());
  if (old_op_log_zone->GetCapacityLeft()) old_op_log_zone->Finish();

  op_log_.reset(new_op_log);

  std::string super_string;
  op_super_block_->EncodeTo(&super_string);

  s = op_log_->AddRecord(super_string);
  if (!s.ok()) {
    Error(logger_,
          "Could not write super block when rolling to a new op log zone");
    return IOStatus::IOError("Failed writing a new superblock");
  }

  s = WriteSnapshotLocked(op_log_.get());

  /* We've rolled successfully, we can reset the old zone now */
  if (s.ok()) {
    auto t = std::thread([&, old_op_log_zone]() {
      std::unique_lock<std::mutex> lk(zbd_->metazone_reset_mtx_);
      old_op_log_zone->Reset();
      zbd_->metazone_reset_cv_.notify_one();
    });
    t.detach();
  }

  auto new_op_log_zone_size =
      op_log_->GetZone()->wp_ - op_log_->GetZone()->start_;
  Info(logger_, "Size of new op log zone %ld\n", new_op_log_zone_size);

  zbd_->roll_throughput_reporter_.AddCount(new_op_log_zone_size);
  return s;
}

IOStatus ZenFS::PersistSnapshot(ZenMetaLog* meta_writer) {
  IOStatus s;

  files_mtx_.lock();
  metadata_sync_mtx_.lock();

  s = WriteSnapshotLocked(meta_writer);
  if (s == IOStatus::NoSpace()) {
    Info(logger_, "Current meta zone full, rolling to next meta zone");
    s = RollMetaZone();
  }

  if (!s.ok()) {
    Error(logger_,
          "Failed persisting a snapshot, we should go to read only now!");
  }

  metadata_sync_mtx_.unlock();
  files_mtx_.unlock();

  return s;
}

IOStatus ZenFS::PersistRecord(std::string record) {
  IOStatus s;

  metadata_sync_mtx_.lock();
  s = op_log_->AddRecord(record);
  if (s == IOStatus::NoSpace()) {
    Info(logger_, "Current meta zone full, rolling to next meta zone");
    s = RollMetaZone();
    /* After a successfull roll, a complete snapshot has been persisted
     * - no need to write the record update */
  }
  metadata_sync_mtx_.unlock();

  return s;
}

IOStatus ZenFS::SyncFileMetadata(ZoneFile* zoneFile) {
  std::string fileRecord;
  std::string output;

  IOStatus s;

  files_mtx_.lock();

  zoneFile->SetFileModificationTime(time(0));
  PutFixed32(&output, kFileUpdate);
  zoneFile->EncodeUpdateTo(&fileRecord);
  PutLengthPrefixedSlice(&output, Slice(fileRecord));

  s = PersistRecord(output);
  if (s.ok()) zoneFile->MetadataSynced();

  files_mtx_.unlock();

  return s;
}

/* Must hold files_mtx_ */
ZoneFile* ZenFS::GetFileInternal(std::string fname) {
  ZoneFile* zoneFile = nullptr;
  if (files_.find(fname) != files_.end()) {
    zoneFile = files_[fname];
  }
  return zoneFile;
}

ZoneFile* ZenFS::GetFile(std::string fname) {
  ZoneFile* zoneFile = nullptr;
  files_mtx_.lock();
  zoneFile = GetFileInternal(fname);
  files_mtx_.unlock();
  return zoneFile;
}

IOStatus ZenFS::DeleteFile(std::string fname) {
  ZoneFile* zoneFile = nullptr;
  IOStatus s;

  files_mtx_.lock();
  zoneFile = GetFileInternal(fname);
  if (zoneFile != nullptr) {
    std::string record;

    zoneFile = files_[fname];
    files_.erase(fname);

    EncodeFileDeletionTo(zoneFile, &record);
    s = PersistRecord(record);
    if (!s.ok()) {
      /* Failed to persist the delete, return to a consistent state */
      files_.insert(std::make_pair(fname.c_str(), zoneFile));
    } else {
      delete (zoneFile);
    }
  }
  files_mtx_.unlock();
  return s;
}

IOStatus ZenFS::NewSequentialFile(const std::string& fname,
                                  const FileOptions& file_opts,
                                  std::unique_ptr<FSSequentialFile>* result,
                                  IODebugContext* dbg) {
  ZoneFile* zoneFile = GetFile(fname);

  Debug(logger_, "New sequential file: %s direct: %d\n", fname.c_str(),
        file_opts.use_direct_reads);

  if (zoneFile == nullptr) {
    return target()->NewSequentialFile(ToAuxPath(fname), file_opts, result,
                                       dbg);
  }

  result->reset(new ZonedSequentialFile(zoneFile, file_opts));
  return IOStatus::OK();
}

IOStatus ZenFS::NewRandomAccessFile(const std::string& fname,
                                    const FileOptions& file_opts,
                                    std::unique_ptr<FSRandomAccessFile>* result,
                                    IODebugContext* dbg) {
  ZoneFile* zoneFile = GetFile(fname);

  Debug(logger_, "New random access file: %s direct: %d\n", fname.c_str(),
        file_opts.use_direct_reads);

  if (zoneFile == nullptr) {
    return target()->NewRandomAccessFile(ToAuxPath(fname), file_opts, result,
                                         dbg);
  }

  result->reset(new ZonedRandomAccessFile(files_[fname], file_opts));
  return IOStatus::OK();
}

IOStatus ZenFS::NewWritableFile(const std::string& fname,
                                const FileOptions& file_opts,
                                std::unique_ptr<FSWritableFile>* result,
                                IODebugContext* /*dbg*/) {
  ZoneFile* zoneFile;
  IOStatus s;

  Debug(logger_, "New writable file: %s direct: %d\n", fname.c_str(),
        file_opts.use_direct_writes);

  if (GetFile(fname) != nullptr) {
    s = DeleteFile(fname);
    if (!s.ok()) return s;
  }

  zoneFile = new ZoneFile(zbd_, fname, next_file_id_++, logger_);
  zoneFile->SetFileModificationTime(time(0));

  /* Persist the creation of the file */
  s = SyncFileMetadata(zoneFile);
  if (!s.ok()) {
    delete zoneFile;
    return s;
  }

  files_mtx_.lock();
  files_.insert(std::make_pair(fname.c_str(), zoneFile));
  files_mtx_.unlock();

  result->reset(new ZonedWritableFile(zbd_, !file_opts.use_direct_writes,
                                      zoneFile, &metadata_writer_));

  return s;
}

IOStatus ZenFS::ReuseWritableFile(const std::string& fname,
                                  const std::string& old_fname,
                                  const FileOptions& file_opts,
                                  std::unique_ptr<FSWritableFile>* result,
                                  IODebugContext* dbg) {
  Debug(logger_, "Reuse writable file: %s old name: %s\n", fname.c_str(),
        old_fname.c_str());

  if (GetFile(fname) == nullptr)
    return IOStatus::NotFound("Old file does not exist");

  return NewWritableFile(fname, file_opts, result, dbg);
}

IOStatus ZenFS::FileExists(const std::string& fname, const IOOptions& options,
                           IODebugContext* dbg) {
  Debug(logger_, "FileExists: %s \n", fname.c_str());

  if (GetFile(fname) == nullptr) {
    return target()->FileExists(ToAuxPath(fname), options, dbg);
  } else {
    return IOStatus::OK();
  }
}

IOStatus ZenFS::ReopenWritableFile(const std::string& fname,
                                   const FileOptions& options,
                                   std::unique_ptr<FSWritableFile>* result,
                                   IODebugContext* dbg) {
  Debug(logger_, "Reopen writable file: %s \n", fname.c_str());

  if (GetFile(fname) != nullptr)
    return NewWritableFile(fname, options, result, dbg);

  return target()->NewWritableFile(fname, options, result, dbg);
}

IOStatus ZenFS::GetChildren(const std::string& dir, const IOOptions& options,
                            std::vector<std::string>* result,
                            IODebugContext* dbg) {
  std::map<std::string, ZoneFile*>::iterator it;
  std::vector<std::string> auxfiles;
  IOStatus s;

  Debug(logger_, "GetChildren: %s \n", dir.c_str());

  s = target()->GetChildren(ToAuxPath(dir), options, &auxfiles, dbg);
  if (!s.ok()) {
    return s;
  }

  for (const auto& f : auxfiles) {
    if (f != "." && f != "..") result->push_back(f);
  }

  files_mtx_.lock();
  for (it = files_.begin(); it != files_.end(); it++) {
    std::string fname = it->first;
    if (fname.rfind(dir, 0) == 0) {
      if (dir.back() == '/') {
        fname.erase(0, dir.length());
      } else {
        fname.erase(0, dir.length() + 1);
      }
      // Don't report grandchildren
      if (fname.find("/") == std::string::npos) {
        result->push_back(fname);
      }
    }
  }
  files_mtx_.unlock();

  return s;
}

IOStatus ZenFS::DeleteFile(const std::string& fname, const IOOptions& options,
                           IODebugContext* dbg) {
  IOStatus s;
  ZoneFile* zoneFile = GetFile(fname);

  Debug(logger_, "Delete file: %s \n", fname.c_str());

  if (zoneFile == nullptr) {
    return target()->DeleteFile(ToAuxPath(fname), options, dbg);
  }

  if (zoneFile->IsOpenForWR()) {
    s = IOStatus::Busy("Cannot delete, file open for writing: ", fname.c_str());
  } else {
    s = DeleteFile(fname);
    zbd_->LogZoneStats();
  }

  return s;
}

IOStatus ZenFS::GetFileModificationTime(const std::string& f,
                                        const IOOptions& options,
                                        uint64_t* mtime, IODebugContext* dbg) {
  ZoneFile* zoneFile;
  IOStatus s;

  Debug(logger_, "GetFileModificationTime: %s \n", f.c_str());
  files_mtx_.lock();
  if (files_.find(f) != files_.end()) {
    zoneFile = files_[f];
    *mtime = (uint64_t)zoneFile->GetFileModificationTime();
  } else {
    s = target()->GetFileModificationTime(ToAuxPath(f), options, mtime, dbg);
  }
  files_mtx_.unlock();
  return s;
}

IOStatus ZenFS::GetFileSize(const std::string& f, const IOOptions& options,
                            uint64_t* size, IODebugContext* dbg) {
  ZoneFile* zoneFile;
  IOStatus s;

  Debug(logger_, "GetFileSize: %s \n", f.c_str());

  files_mtx_.lock();
  if (files_.find(f) != files_.end()) {
    zoneFile = files_[f];
    *size = zoneFile->GetFileSize();
  } else {
    s = target()->GetFileSize(ToAuxPath(f), options, size, dbg);
  }
  files_mtx_.unlock();

  return s;
}

IOStatus ZenFS::RenameFile(const std::string& f, const std::string& t,
                           const IOOptions& options, IODebugContext* dbg) {
  ZoneFile* zoneFile;
  IOStatus s;

  Debug(logger_, "Rename file: %s to : %s\n", f.c_str(), t.c_str());

  zoneFile = GetFile(f);
  if (zoneFile != nullptr) {
    s = DeleteFile(t);
    if (s.ok()) {
      files_mtx_.lock();
      files_.erase(f);
      zoneFile->Rename(t);
      files_.insert(std::make_pair(t, zoneFile));
      files_mtx_.unlock();

      s = SyncFileMetadata(zoneFile);
      if (!s.ok()) {
        /* Failed to persist the rename, roll back */
        files_mtx_.lock();
        files_.erase(t);
        zoneFile->Rename(f);
        files_.insert(std::make_pair(f, zoneFile));
        files_mtx_.unlock();
      }
    }
  } else {
    s = target()->RenameFile(ToAuxPath(f), ToAuxPath(t), options, dbg);
  }

  return s;
}

void ZenFS::EncodeSnapshotTo(std::string* output) {
  std::map<std::string, ZoneFile*>::iterator it;
  std::string files_string;
  PutFixed32(output, kCompleteFilesSnapshot);
  for (it = files_.begin(); it != files_.end(); it++) {
    std::string file_string;
    ZoneFile* zFile = it->second;

    zFile->EncodeSnapshotTo(&file_string);
    PutLengthPrefixedSlice(&files_string, Slice(file_string));
  }
  PutLengthPrefixedSlice(output, Slice(files_string));
}

void ZenFS::EncodeJson(std::ostream& json_stream) {
  bool first_element = true;
  json_stream << "[";
  for (const auto& file : files_) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    file.second->EncodeJson(json_stream);
  }
  json_stream << "]";
}

Status ZenFS::DecodeFileUpdateFrom(Slice* slice) {
  ZoneFile* update = new ZoneFile(zbd_, "not_set", 0, logger_);
  uint64_t id;
  Status s;

  s = update->DecodeFrom(slice);
  if (!s.ok()) return s;

  id = update->GetID();
  if (id >= next_file_id_) next_file_id_ = id + 1;

  /* Check if this is an update to an existing file */
  for (auto it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zFile = it->second;
    if (id == zFile->GetID()) {
      std::string oldName = zFile->GetFilename();

      s = zFile->MergeUpdate(update);
      delete update;

      if (!s.ok()) return s;

      if (zFile->GetFilename() != oldName) {
        files_.erase(oldName);
        files_.insert(std::make_pair(zFile->GetFilename(), zFile));
      }

      return Status::OK();
    }
  }

  /* The update is a new file */
  assert(GetFile(update->GetFilename()) == nullptr);
  files_.insert(std::make_pair(update->GetFilename(), update));

  return Status::OK();
}

Status ZenFS::DecodeSnapshotFrom(Slice* input) {
  Slice slice;

  assert(files_.size() == 0);

  while (GetLengthPrefixedSlice(input, &slice)) {
    ZoneFile* zoneFile = new ZoneFile(zbd_, "not_set", 0, logger_);
    Status s = zoneFile->DecodeFrom(&slice);
    if (!s.ok()) return s;

    files_.insert(std::make_pair(zoneFile->GetFilename(), zoneFile));
    if (zoneFile->GetID() >= next_file_id_)
      next_file_id_ = zoneFile->GetID() + 1;
  }

  return Status::OK();
}

void ZenFS::EncodeFileDeletionTo(ZoneFile* zoneFile, std::string* output) {
  std::string file_string;

  PutFixed64(&file_string, zoneFile->GetID());
  PutLengthPrefixedSlice(&file_string, Slice(zoneFile->GetFilename()));

  PutFixed32(output, kFileDeletion);
  PutLengthPrefixedSlice(output, Slice(file_string));
}

Status ZenFS::DecodeFileDeletionFrom(Slice* input) {
  uint64_t fileID;
  std::string fileName;
  Slice slice;

  if (!GetFixed64(input, &fileID))
    return Status::Corruption("Zone file deletion: file id missing");

  if (!GetLengthPrefixedSlice(input, &slice))
    return Status::Corruption("Zone file deletion: file name missing");

  fileName = slice.ToString();
  if (files_.find(fileName) == files_.end())
    return Status::Corruption("Zone file deletion: no such file");

  ZoneFile* zoneFile = files_[fileName];
  if (zoneFile->GetID() != fileID)
    return Status::Corruption("Zone file deletion: file ID missmatch");

  files_.erase(fileName);
  delete zoneFile;

  return Status::OK();
}

Status ZenFS::RecoverFromSnapshotZone(ZenMetaLog* log) {
  return RecoverFrom(log, RecoverType::kSNAPSHOT);
}

Status ZenFS::RecoverFromOpLogZone(ZenMetaLog* log) {
  return RecoverFrom(log, RecoverType::kOPLOG);
}

Status ZenFS::RecoverFrom(ZenMetaLog* log, RecoverType type) {
  bool at_least_one_snapshot = false;
  bool at_least_one_oplog = false;

  std::string scratch;
  uint32_t tag = 0;
  Slice record;
  Slice data;
  Status s;

  while (true) {
    IOStatus rs = log->ReadRecord(&record, &scratch);
    if (!rs.ok()) {
      Error(logger_, "Read recovery record failed with error: %s",
            rs.ToString().c_str());
      return Status::Corruption("ZenFS", "Metadata corruption");
    }

    if (!GetFixed32(&record, &tag)) break;

    if (tag == kEndRecord) break;

    if (!GetLengthPrefixedSlice(&record, &data)) {
      return Status::Corruption("ZenFS", "No recovery record data");
    }

    switch (tag) {
      case kCompleteFilesSnapshot:
        ClearFiles();
        s = DecodeSnapshotFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode complete snapshot: %s",
               s.ToString().c_str());
          return s;
        }
        // In the latest implementation, current zone may contains
        // multiple snapshot records, we only need the latest one.
        // Since each time we enter this swtich case, the file_ map
        // is cleared, so its OK to decode snapshot multiple times.
        at_least_one_snapshot = true;
        break;

      case kFileUpdate:
        s = DecodeFileUpdateFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode file snapshot: %s",
               s.ToString().c_str());
          return s;
        }
        at_least_one_oplog = true;
        break;

      case kFileDeletion:
        s = DecodeFileDeletionFrom(&data);
        if (!s.ok()) {
          Warn(logger_, "Could not decode file deletion: %s",
               s.ToString().c_str());
          return s;
        }
        at_least_one_oplog = true;
        break;

      default:
        Warn(logger_, "Unexpected metadata record tag: %u", tag);
        return Status::Corruption("ZenFS", "Unexpected tag");
    }
  }

  if (at_least_one_snapshot && type == RecoverType::kSNAPSHOT) {
    return Status::OK();
  }

  if (at_least_one_oplog && type == RecoverType::kOPLOG) {
    return Status::OK();
  }

  if ((at_least_one_oplog || at_least_one_snapshot) && RecoverType::kBOTH) {
    return Status::OK();
  }

  return Status::NotFound("ZenFS", "No snapshot found");
}

#define ZENV_URI_PATTERN "zenfs://"

/* find all valid superblocks for snapshot or op log zones */
Status ZenFS::FindAllValidSuperblocks(
    ZenFSZoneTag zone_tag,
    std::vector<std::unique_ptr<Superblock>>& valid_superblocks,
    std::vector<std::unique_ptr<ZenMetaLog>>& valid_logs,
    std::vector<Zone*>& valid_zones,
    std::vector<std::pair<uint32_t, uint32_t>>& seq_map) {
  std::vector<Zone*> zones;

  switch (zone_tag) {
    case kSnapshotZone: {
      zones = zbd_->GetSnapshotZones();
      break;
    }
    case kOpLogZone: {
      zones = zbd_->GetOpZones();
      break;
    }
    default:
      Warn(logger_, "Unexpected ZenFS zone tag: %u", zone_tag);
      return Status::Corruption("ZenFS", "Unexpected zone tag");
  }

  Status s;

  /* We need a minimum of two non-offline zones */
  if (zones.size() < 2) {
    Error(logger_, "Need at least two non-offline zones to open for write");
    return Status::NotSupported();
  }

  /* Find all valid superblocks in the zones*/
  for (const auto z : zones) {
    std::unique_ptr<ZenMetaLog> log;
    std::string scratch;
    Slice super_record;

    log.reset(new ZenMetaLog(zbd_, z));

    if (!log->ReadRecord(&super_record, &scratch).ok()) continue;

    if (super_record.size() == 0) continue;

    std::unique_ptr<Superblock> super_block;

    super_block.reset(new Superblock());
    s = super_block->DecodeFrom(&super_record);
    if (s.ok()) s = super_block->CompatibleWith(zbd_);
    if (!s.ok()) return s;

    Info(logger_, "Found OK superblock in the zone %lu seq: %u\n",
         z->GetZoneNr(), super_block->GetSeq());

    seq_map.push_back(std::make_pair(super_block->GetSeq(), seq_map.size()));
    valid_superblocks.push_back(std::move(super_block));
    valid_logs.push_back(std::move(log));
    valid_zones.push_back(z);
  }

  if (!seq_map.size()) {
    return Status::NotFound("No valid superblock found from the zones");
  }

  /* Sort superblocks by descending sequence number in the zones*/
  std::sort(seq_map.begin(), seq_map.end(),
            std::greater<std::pair<uint32_t, uint32_t>>());

  return Status::OK();
}

/* Mount the filesystem by recovering form the latest valid snapshot zone
 * and metadata zone*/
Status ZenFS::Mount(bool readonly) {
  Status s;

#ifdef WITH_ZENFS_ASYNC_METAZONE_ROLLOVER
  std::vector<std::unique_ptr<Superblock>> valid_superblocks_snapshot;
  std::vector<std::unique_ptr<ZenMetaLog>> valid_logs_snapshot;
  std::vector<Zone*> valid_zones_snapshot;
  std::vector<std::pair<uint32_t, uint32_t>> seq_map_snapshot;

  s = FindAllValidSuperblocks(kSnapshotZone, valid_superblocks_snapshot,
                              valid_logs_snapshot, valid_zones_snapshot,
                              seq_map_snapshot);
  if (!s.ok()) {
    Error(logger_, "Did not find valid superblock in Snapshot zones. Error: %s",
          s.ToString().c_str());
    return s;
  }

  bool snapshot_recovery_ok = false;
  unsigned int snapshot_recovered_index = 0;

  /* Recover from the zone with the highest superblock sequence number.
     If that fails go to the previous as we might have crashed when rolling
     snapshot zone.
  */
  for (const auto& sm : seq_map_snapshot) {
    uint32_t i = sm.second;
    std::string scratch;
    std::unique_ptr<ZenMetaLog> log = std::move(valid_logs_snapshot[i]);

    s = RecoverFromSnapshotZone(log.get());
    if (!s.ok()) {
      if (s.IsNotFound()) {
        Warn(logger_,
             "Did not find a valid snapshot, trying next snapshot zone. "
             "Error: %s",
             s.ToString().c_str());
        continue;
      }

      Error(logger_, "Snapshot corruption. Error: %s", s.ToString().c_str());
      return s;
    }

    snapshot_recovered_index = i;
    snapshot_recovery_ok = true;
    snapshot_log_ = std::move(log);
    break;
  }

  if (!snapshot_recovery_ok) {
    return Status::IOError(
        "Failed to mount filesystem due to "
        "snapshot recovery");
  }

  Info(logger_, "Recovered from snapshot zone: %d",
       (int)valid_zones_snapshot[snapshot_recovered_index]->GetZoneNr());
  snapshot_super_block_ =
      std::move(valid_superblocks_snapshot[snapshot_recovered_index]);

  /* Free up old snapshot zones, to get ready to roll */
  for (const auto& sm : seq_map_snapshot) {
    uint32_t i = sm.second;
    /* Don't reset the current snapshot zone */
    if (i != snapshot_recovered_index) {
      /* snapshot zones are not marked as having valid data, so they can be
       * reset */
      valid_logs_snapshot[i].reset();
    }
  }

#endif  // WITH_ZENFS_ASYNC_METAZONE_ROLLOVER

  std::vector<std::unique_ptr<Superblock>> valid_superblocks_op;
  std::vector<std::unique_ptr<ZenMetaLog>> valid_logs_op;
  std::vector<Zone*> valid_zones_op_log;
  std::vector<std::pair<uint32_t, uint32_t>> seq_map_op;

  s = FindAllValidSuperblocks(kOpLogZone, valid_superblocks_op, valid_logs_op,
                              valid_zones_op_log, seq_map_op);
  if (!s.ok()) {
    Error(logger_, "Did not find valid superblock in Op Log zones. Error: %s",
          s.ToString().c_str());
    return s;
  }

  bool op_zone_recovery_ok = false;
  unsigned int op_zone_recovered_index = 0;

  /* Recover from the zone with the highest superblock sequence number.
     If that fails go to the previous as we might have crashed when rolling
     metadata zone.
  */
  for (const auto& sm : seq_map_op) {
    uint32_t i = sm.second;
    std::string scratch;
    std::unique_ptr<ZenMetaLog> log = std::move(valid_logs_op[i]);

#ifdef WITH_ZENFS_ASYNC_METAZONE_ROLLOVER
    s = RecoverFromOpLogZone(log.get());
#else
    s = RecoverFrom(log.get());
#endif  // WITH_ZENFS_ASYNC_METAZONE_ROLLOVER

    if (!s.ok()) {
      if (s.IsNotFound()) {
        Warn(logger_,
             "Did not find a valid op log, trying next op log zone. Error: %s",
             s.ToString().c_str());
        continue;
      }

      Error(logger_, "op log corruption. Error: %s", s.ToString().c_str());
      return s;
    }

    op_zone_recovered_index = i;
    op_zone_recovery_ok = true;
    op_log_ = std::move(log);
    break;
  }

  if (!op_zone_recovery_ok) {
    return Status::IOError(
        "Failed to mount filesystem due to "
        "op log zone recovery");
  }

  Info(logger_, "Recovered from zone: %d",
       (int)valid_zones_op_log[op_zone_recovered_index]->GetZoneNr());
  op_super_block_ = std::move(valid_superblocks_op[op_zone_recovered_index]);

  zbd_->SetFinishTreshold(op_super_block_->GetFinishTreshold());
  zbd_->SetMaxActiveZones(op_super_block_->GetMaxActiveZoneLimit());
  zbd_->SetMaxOpenZones(op_super_block_->GetMaxOpenZoneLimit());

  IOOptions foo;
  IODebugContext bar;
  s = target()->CreateDirIfMissing(op_super_block_->GetAuxFsPath(), foo, &bar);
  if (!s.ok()) {
    Error(logger_, "Failed to create aux filesystem directory.");
    return s;
  }

  /* Free up old op log zones, to get ready to roll */
  for (const auto& sm : seq_map_op) {
    uint32_t i = sm.second;
    /* Don't reset the current op log zone */
    if (i != op_zone_recovered_index) {
      /* op log zones are not marked as having valid data, so they can be
       * reset */
      valid_logs_op[i].reset();
    }
  }

  if (readonly) {
    Info(logger_, "Mounting READ ONLY");
  } else {
    files_mtx_.lock();
    s = RollMetaZone();
    if (!s.ok()) {
      files_mtx_.unlock();
      Error(logger_, "Failed to roll metadata zone.");
      return s;
    }
    files_mtx_.unlock();
  }

  Info(logger_, "Superblock sequence %d", (int)op_super_block_->GetSeq());
  Info(logger_, "Finish threshold %u", op_super_block_->GetFinishTreshold());
  Info(logger_, "Filesystem mount OK");

  if (!readonly) {
    Info(logger_, "Resetting unused IO Zones..");
    zbd_->ResetUnusedIOZones();
    Info(logger_, "  Done");
  }

  LogFiles();

  return Status::OK();
}

Status ZenFS::InitMetaZone(
    std::vector<Zone*> const& zones, std::unique_ptr<ZenMetaLog>* log) {
  Status s;
  Zone* reset_zone = nullptr;

  for (const auto z : zones) {
    if (z->Reset().ok()) {
      if (!reset_zone) reset_zone = z;
    } else {
      Warn(logger_, "Failed to reset the zone\n");
    }
  }

  if (!reset_zone) {
    return Status::IOError("No available zones\n");
  }

  log->reset(new ZenMetaLog(zbd_, reset_zone));
}

Status ZenFS::CreateEmptySuperBlock(std::unique_ptr<ZenMetaLog>* log,
                                    std::string const& aux_fs_path,
                                    uint32_t const finish_threshold,
                                    uint32_t const max_open_limit,
                                    uint32_t const max_active_limit) {
  Status s;
  Superblock* super = new Superblock(zbd_, aux_fs_path, finish_threshold,
                                     max_open_limit, max_active_limit);
  std::string super_string;
  super->EncodeTo(&super_string);

  // write an empty superblock to the zone
  s = (log->get())->AddRecord(super_string);
  if (!s.ok()) return std::move(s);

  return Status::OK();
}

Status ZenFS::MkFS(std::string aux_fs_path, uint32_t finish_threshold,
                   uint32_t max_open_limit, uint32_t max_active_limit) {
  Status s;

  std::vector<Zone*> op_zones = zbd_->GetOpZones();
  std::unique_ptr<ZenMetaLog> op_log;
  Zone* reset_op_log_zone = nullptr;

  /* TODO: check practical limits */

  if (max_open_limit > zbd_->GetMaxOpenZones()) {
    return Status::InvalidArgument(
        "Max open zone limit exceeds the device limit\n");
  }

  if (max_active_limit > zbd_->GetMaxActiveZones()) {
    return Status::InvalidArgument(
        "Max active zone limit exceeds the device limit\n");
  }

  if (max_active_limit < max_open_limit) {
    return Status::InvalidArgument(
        "Max open limit must be smaller than max active limit\n");
  }

  if (aux_fs_path.length() > 255) {
    return Status::InvalidArgument(
        "Aux filesystem path must be less than 256 bytes\n");
  }

  ClearFiles();
  zbd_->ResetUnusedIOZones();

#ifdef WITH_ZENFS_ASYNC_METAZONE_ROLLOVER
  std::vector<Zone*> snapshot_zones = zbd_->GetSnapshotZones();
  std::unique_ptr<ZenMetaLog> snapshot_log;

  s = InitMetaZone(snapshot_zones, &snapshot_log);
  if (!s.ok()) {
    Error(logger_, "Failed to reset snapshot: %s", s.ToString().c_str());
    return Status::IOError("Failed to reset snapshot");
  }

  s = CreateEmptySuperBlock(&snapshot_log, aux_fs_path, finish_threshold,
                            max_open_limit, max_active_limit);
  if (!s.ok()) {
    Error(logger_, "Failed to create empty super block in snapshot: %s",
          s.ToString().c_str());
    return Status::IOError("create empty super block in snapshot: " +
                           s.ToString());
  }

  /* Write an empty snapshot to make the snapshot zone valid */
  s = PersistSnapshot(snapshot_log.get());
  if (!s.ok()) {
    Error(logger_, "Failed to persist snapshot: %s", s.ToString().c_str());
    return Status::IOError("Failed persist snapshot");
  }
#endif  // WITH_ZENFS_ASYNC_METAZONE_ROLLOVER

  s = InitMetaZone(op_zones, &op_log);
  if (!s.ok()) {
    Error(logger_, "Failed to reset op log: %s", s.ToString().c_str());
    return Status::IOError("Failed to reset op log");
  }

  s = CreateEmptySuperBlock(&op_log, aux_fs_path, finish_threshold,
                            max_open_limit, max_active_limit);
  if (!s.ok()) {
    Error(logger_, "Failed to create empty super block in op log zone: %s",
          s.ToString().c_str());
    return Status::IOError("create empty super block in op log zone: " +
                           s.ToString());
  }

#ifndef WITH_ZENFS_ASYNC_METAZONE_ROLLOVER
  /* Write an empty snapshot to make the metadata zone valid */
  s = PersistSnapshot(op_log.get());
  if (!s.ok()) {
    Error(logger_, "Failed to persist snapshot in log op zone: %s",
          s.ToString().c_str());
    return Status::IOError("Failed persist snapshot in log op zone");
  }
#endif  // WITH_ZENFS_ASYNC_METAZONE_ROLLOVER

  Info(logger_, "Empty filesystem created");
  return Status::OK();
}

std::map<std::string, Env::WriteLifeTimeHint> ZenFS::GetWriteLifeTimeHints() {
  std::map<std::string, Env::WriteLifeTimeHint> hint_map;

  for (auto it = files_.begin(); it != files_.end(); it++) {
    ZoneFile* zoneFile = it->second;
    std::string filename = it->first;
    hint_map.insert(std::make_pair(filename, zoneFile->GetWriteLifeTimeHint()));
  }

  return hint_map;
}

std::vector<ZoneStat> ZenFS::GetStat() {
  // Store size of each file_id in each zone
  std::map<uint64_t, std::map<uint64_t, uint64_t>> sizes;
  // Store file_id to filename map
  std::map<uint64_t, std::string> filenames;

  files_mtx_.lock();

  for (auto& file_it : files_) {
    ZoneFile* file = file_it.second;
    uint64_t file_id = file->GetID();
    filenames[file_id] = file->GetFilename();
    for (ZoneExtent* extent : file->GetExtents()) {
      uint64_t zone_fake_id = extent->zone_->start_;
      sizes[zone_fake_id][file_id] += extent->length_;
    }
  }

  files_mtx_.unlock();

  // Final result vector
  std::vector<ZoneStat> stat = zbd_->GetStat();

  for (auto& zone : stat) {
    std::map<uint64_t, uint64_t>& zone_files = sizes[zone.start_position];
    for (auto& file : zone_files) {
      uint64_t file_id = file.first;
      uint64_t file_length = file.second;
      ZoneFileStat file_stat;
      file_stat.file_id = file_id;
      file_stat.size_in_zone = file_length;
      file_stat.filename = filenames[file_id];
      zone.files.emplace_back(std::move(file_stat));
    }
  }

  return stat;
};

static std::string GetLogFilename(std::string bdev) {
  std::ostringstream ss;
  time_t t = time(0);
  struct tm* log_start = std::localtime(&t);
  char buf[40];

  std::strftime(buf, sizeof(buf), "%Y-%m-%d_%H:%M:%S.log", log_start);
  ss << DEFAULT_ZENV_LOG_PATH << std::string("zenfs_") << bdev << "_" << buf;

  return ss.str();
}

Status NewZenFS(
    FileSystem** fs, const std::string& bdevname, std::string bytedance_tags_,
    std::shared_ptr<MetricsReporterFactory> metrics_reporter_factory_) {
  std::shared_ptr<Logger> logger;
  Status s;

  s = Env::Default()->NewLogger(GetLogFilename(bdevname), &logger);
  if (!s.ok()) {
    fprintf(stderr, "ZenFS: Could not create logger");
  } else {
    logger->SetInfoLogLevel(DEBUG_LEVEL);
  }
  ZonedBlockDevice* zbd = new ZonedBlockDevice(
      bdevname, logger, bytedance_tags_, metrics_reporter_factory_);
  IOStatus zbd_status = zbd->Open();
  if (!zbd_status.ok()) {
    Error(logger, "Failed to open zoned block device: %s",
          zbd_status.ToString().c_str());
    return Status::IOError(zbd_status.ToString());
  }

  ZenFS* zenFS = new ZenFS(zbd, FileSystem::Default(), logger);
  s = zenFS->Mount(false);
  if (!s.ok()) {
    delete zenFS;
    return s;
  }

  *fs = zenFS;
  return Status::OK();
}

std::map<std::string, std::string> ListZenFileSystems() {
  std::map<std::string, std::string> zenFileSystems;
  DIR* dir = opendir("/sys/class/block");
  struct dirent* entry;

  while (NULL != (entry = readdir(dir))) {
    if (entry->d_type == DT_LNK) {
      std::string zbdName = std::string(entry->d_name);
      ZonedBlockDevice* zbd = new ZonedBlockDevice(zbdName, nullptr);
      IOStatus zbd_status = zbd->Open(true);

      if (zbd_status.ok()) {
        std::vector<Zone*> op_zones = zbd->GetOpZones();
        std::string scratch;
        Slice super_record;
        Status s;

        for (const auto z : op_zones) {
          Superblock super_block;
          std::unique_ptr<ZenMetaLog> log;
          log.reset(new ZenMetaLog(zbd, z));

          if (!log->ReadRecord(&super_record, &scratch).ok()) continue;
          s = super_block.DecodeFrom(&super_record);
          if (s.ok()) {
            /* Map the uuid to the device-mapped (i.g dm-linear) block device to
               avoid trying to mount the whole block device in case of a split
               device */
            if (zenFileSystems.find(super_block.GetUUID()) !=
                    zenFileSystems.end() &&
                zenFileSystems[super_block.GetUUID()].rfind("dm-", 0) == 0) {
              break;
            }
            zenFileSystems[super_block.GetUUID()] = zbdName;
            break;
          }
        }

        delete zbd;
        continue;
      }
    }
  }

  return zenFileSystems;
}

extern "C" FactoryFunc<FileSystem> zenfs_filesystem_reg;

FactoryFunc<FileSystem> zenfs_filesystem_reg =
    ObjectLibrary::Default()->Register<FileSystem>(
        "zenfs://.*", [](const std::string& uri, std::unique_ptr<FileSystem>* f,
                         std::string* errmsg) {
          std::string devID = uri;
          FileSystem* fs = nullptr;
          Status s;

          devID.replace(0, strlen("zenfs://"), "");
          if (devID.rfind("dev:") == 0) {
            devID.replace(0, strlen("dev:"), "");
            s = NewZenFS(&fs, devID, "zenfs-testing",
                         std::make_shared<ByteDanceMetricsReporterFactory>());
            std::cerr << "Metrics is not enabled due to using zenfs:// path"
                      << std::endl;
            if (!s.ok()) {
              *errmsg = s.ToString();
            }
          } else if (devID.rfind("uuid:") == 0) {
            std::map<std::string, std::string> zenFileSystems =
                ListZenFileSystems();
            devID.replace(0, strlen("uuid:"), "");

            if (zenFileSystems.find(devID) == zenFileSystems.end()) {
              *errmsg = "UUID not found";
            } else {
              s = NewZenFS(&fs, zenFileSystems[devID], "zenfs-testing",
                           std::make_shared<ByteDanceMetricsReporterFactory>());
              std::cerr << "Metrics is not enabled due to using zenfs:// path"
                        << std::endl;
              if (!s.ok()) {
                *errmsg = s.ToString();
              }
            }
          } else {
            *errmsg = "Malformed URI";
          }
          f->reset(fs);
          return f->get();
        });
};  // namespace ROCKSDB_NAMESPACE

#else

#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {
Status NewZenFS(
    FileSystem** /*fs*/, const std::string& /*bdevname*/,
    std::string /*bytedance_tags_*/,
    std::shared_ptr<MetricsReporterFactory> /*metrics_reporter_factory_*/) {
  return Status::NotSupported("Not built with ZenFS support\n");
}
std::map<std::string, std::string> ListZenFileSystems() {
  std::map<std::string, std::string> zenFileSystems;
  return zenFileSystems;
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
