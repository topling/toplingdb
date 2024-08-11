//  Copyright (c) Topling, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "fs_cat.h"
#include <file/file_util.h>
#include <file/writable_file_writer.h>
#include <terark/util/function.hpp>

namespace ROCKSDB_NAMESPACE {

IOStatus CopyAcrossFS(FileSystem* dest, FileSystem* src,
                      const std::string& fname, IODebugContext* dbg) {
  std::unique_ptr<FSWritableFile> dest_file;
  FileOptions file_opt;
  IOStatus ios = dest->NewWritableFile(fname, file_opt, &dest_file, dbg);
  if (ios.ok()) {
    auto dest_writer = std::make_unique<WritableFileWriter>(
                   std::move(dest_file), fname, file_opt);
    ios = CopyFile(src, fname, dest_writer, 0/*filesize*/,
                   false/*use_fsync*/, nullptr/*io_tracer*/,
                   Temperature::kWarm);
  }
  return ios;
}
template<class FileSystemPtr>
IOStatus CopyAcrossFS(const FileSystemPtr& dest, const FileSystemPtr src,
                      const std::string& fname, IODebugContext* dbg) {
  return CopyAcrossFS(dest.get(), src.get(), fname, dbg);
}

bool CatFileSystem::IsInstanceOf(const std::string& id) const {
  if (id == kClassName()) {
    return true;
  } else {
    return FileSystem::IsInstanceOf(id);
  }
}

CatFileSystem::CatFileSystem(const std::shared_ptr<FileSystem>& local,
                             const std::shared_ptr<FileSystem>& remote,
                             bool use_mmap_reads)
 : m_local(local), m_remote(remote), m_use_mmap_reads(use_mmap_reads)
{
  ROCKSDB_VERIFY(local != nullptr);
  ROCKSDB_VERIFY(remote != nullptr);
}

Status CatFileSystem::RegisterDbPaths(const std::vector<std::string>& paths) {
  Status s = m_local->RegisterDbPaths(paths);
  if (!s.ok()) return s;
  return m_remote->RegisterDbPaths(paths);
}

Status CatFileSystem::UnregisterDbPaths(const std::vector<std::string>& paths) {
  Status s = m_local->UnregisterDbPaths(paths);
  if (!s.ok()) return s;
  return m_remote->UnregisterDbPaths(paths);
}

IOStatus CatFileSystem::NewSequentialFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSSequentialFile>* result, IODebugContext* dbg) {
  IOStatus ios = m_local->NewSequentialFile(fname, options, result, dbg);
  if (!ios.ok()) {
    ios = m_local->FileExists(fname, options.io_options, dbg);
  }
  if (ios.IsNotFound()) {
    ios = CopyAcrossFS(m_local, m_remote, fname, dbg);
    if (ios.ok()) {
      ios = m_local->NewSequentialFile(fname, options, result, dbg);
    }
  }
  return ios;
}

IOStatus CatFileSystem::NewRandomAccessFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  auto do_open = [&](const FileOptions& opt) {
    IOStatus ios = m_local->NewRandomAccessFile(fname, opt, result, dbg);
    if (!ios.ok()) {
      ios = m_local->FileExists(fname, opt.io_options, dbg);
    }
    if (ios.IsNotFound()) {
      ios = CopyAcrossFS(m_local, m_remote, fname, dbg);
      if (ios.ok()) {
        ios = m_local->NewRandomAccessFile(fname, opt, result, dbg);
      }
    }
    return ios;
  };
  if (m_use_mmap_reads && !options.use_mmap_reads) {
    FileOptions opt2 = options;
    opt2.use_mmap_reads = true;
    return do_open(opt2);
  }
  return do_open(options);
}

class WritableFileCat : public FSWritableFile {
 public:
  std::unique_ptr<FSWritableFile> m_local, m_remote;
  std::string fname;

  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override {
    IOStatus ios = m_local->Append(data, options, dbg);
    if (ios.ok()) {
      ios = m_remote->Append(data, options, dbg);
    }
    return ios;
  }
  IOStatus Append(const Slice& data, const IOOptions& options,
                  const DataVerificationInfo& verification_info,
                  IODebugContext* dbg) override {
    IOStatus ios = m_local->Append(data, options, verification_info, dbg);
    if (ios.ok()) {
      ios = m_remote->Append(data, options, verification_info, dbg);
    }
    return ios;
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override {
    IOStatus ios = m_local->PositionedAppend(data, offset, options, dbg);
    if (ios.ok()) {
      ios = m_remote->PositionedAppend(data, offset, options, dbg);
    }
    return ios;
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            const DataVerificationInfo& verification_info,
                            IODebugContext* dbg) override {
    IOStatus ios = m_local->PositionedAppend(data, offset, options, verification_info, dbg);
    if (ios.ok()) {
      ios = m_remote->PositionedAppend(data, offset, options, verification_info, dbg);
    }
    return ios;
  }
  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override {
    IOStatus ios = m_local->Truncate(size, options, dbg);
    if (ios.ok()) {
      ios = m_remote->Truncate(size, options, dbg);
    }
    return ios;
  }
  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus ios = m_local->Close(options, dbg);
    if (ios.ok()) {
      ios = m_remote->Close(options, dbg);
    }
    return ios;
  }
  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus ios = m_local->Flush(options, dbg);
    if (ios.ok()) {
      ios = m_remote->Flush(options, dbg);
    }
    return ios;
  }
  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus ios = m_local->Sync(options, dbg);
    if (ios.ok()) {
      ios = m_remote->Sync(options, dbg);
    }
    return ios;
  }
  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus ios = m_local->Fsync(options, dbg);
    if (ios.ok()) {
      ios = m_remote->Fsync(options, dbg);
    }
    return ios;
  }
  bool IsSyncThreadSafe() const override { return m_local->IsSyncThreadSafe(); }

  bool use_direct_io() const override { return m_local->use_direct_io(); }

  size_t GetRequiredBufferAlignment() const override {
    return m_local->GetRequiredBufferAlignment();
  }

  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
    m_local->SetWriteLifeTimeHint(hint);
    m_remote->SetWriteLifeTimeHint(hint);
  }

  Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
    return m_local->GetWriteLifeTimeHint();
  }

  uint64_t GetFileSize(const IOOptions& options, IODebugContext* dbg) override {
    return m_local->GetFileSize(options, dbg);
  }

  void SetPreallocationBlockSize(size_t size) override {
    m_local->SetPreallocationBlockSize(size);
    m_remote->SetPreallocationBlockSize(size);
  }

  void GetPreallocationStatus(size_t* block_size,
                              size_t* last_allocated_block) override {
    m_local->GetPreallocationStatus(block_size, last_allocated_block);
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return m_local->GetUniqueId(id, max_size);
  }

  IOStatus InvalidateCache(size_t offset, size_t length) override {
    IOStatus ios = m_local->InvalidateCache(offset, length);
    if (ios.ok()) {
      ios = m_remote->InvalidateCache(offset, length);
    }
    return ios;
  }

  IOStatus RangeSync(uint64_t offset, uint64_t nbytes,
                     const IOOptions& options,
                     IODebugContext* dbg) override {
    IOStatus ios = m_local->RangeSync(offset, nbytes, options, dbg);
    if (ios.ok()) {
      ios = m_remote->RangeSync(offset, nbytes, options, dbg);
    }
    return ios;
  }

  void PrepareWrite(size_t offset, size_t len, const IOOptions& options,
                    IODebugContext* dbg) override {
    m_local->PrepareWrite(offset, len, options, dbg);
    m_remote->PrepareWrite(offset, len, options, dbg);
  }

  IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions& options,
                    IODebugContext* dbg) override {
    IOStatus ios = m_local->Allocate(offset, len, options, dbg);
    if (ios.ok()) {
      ios = m_remote->Allocate(offset, len, options, dbg);
    }
    return ios;
  }

  intptr_t FileDescriptor() const final { return m_local->FileDescriptor(); }
  void SetFileSize(uint64_t fsize) final {
    m_local->SetFileSize(fsize);
    m_remote->SetFileSize(fsize);
  }
};

IOStatus CatFileSystem::NewWritableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  auto file = new WritableFileCat;
  result->reset(file);
  IOStatus ios = m_local->NewWritableFile(fname, options, &file->m_local, dbg);
  if (!ios.ok()) {
    result->reset(nullptr);
  }
  ios = m_remote->NewWritableFile(fname, options, &file->m_remote, dbg);
  if (!ios.ok()) {
    result->reset(nullptr);
  }
  return ios;
}

IOStatus CatFileSystem::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    const FileOptions& options, std::unique_ptr<FSWritableFile>* result,
    IODebugContext* dbg) {
  ROCKSDB_DIE("Not supported: DBOptions::recycle_log_file_num must be 0");
}

IOStatus CatFileSystem::NewRandomRWFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSRandomRWFile>* result, IODebugContext* dbg) {
  ROCKSDB_DIE(
"Not supported: IngestExternalFileOptions::allow_global_seqno must be false");
}

struct CatFSDirectory : public FSDirectory {
  std::unique_ptr<FSDirectory> m_local, m_remote;
  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus ios = m_local->Fsync(options, dbg);
    if (ios.ok()) {
      ios = m_remote->Fsync(options, dbg);
    }
    return ios;
  }

  IOStatus FsyncWithDirOptions(
      const IOOptions& options, IODebugContext* dbg,
      const DirFsyncOptions& dir_fsync_options) override {
    IOStatus ios = m_local->FsyncWithDirOptions(options, dbg, dir_fsync_options);
    if (ios.ok()) {
      ios = m_remote->FsyncWithDirOptions(options, dbg, dir_fsync_options);
    }
    return ios;
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus ios = m_local->Close(options, dbg);
    if (ios.ok()) {
      ios = m_remote->Close(options, dbg);
    }
    return ios;
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return m_local->GetUniqueId(id, max_size);
  }
};
IOStatus CatFileSystem::NewDirectory(const std::string& dir,
                                       const IOOptions& options,
                                       std::unique_ptr<FSDirectory>* result,
                                       IODebugContext* dbg) {
  auto fsd = std::make_unique<CatFSDirectory>();
  IOStatus ios = m_local->NewDirectory(dir, options, &fsd->m_local, dbg);
  if (ios.ok()) {
    ios = m_remote->NewDirectory(dir, options, &fsd->m_remote, dbg);
  }
  if (ios.ok()) {
    *result = std::move(fsd);
  }
  return ios;
}

IOStatus CatFileSystem::FileExists(const std::string& fname,
                                     const IOOptions& options,
                                     IODebugContext* dbg) {
  IOStatus ios = m_local->FileExists(fname, options, dbg);
  if (!ios.ok()) {
    ios = m_remote->FileExists(fname, options, dbg);
    if (ios.ok()) { // fetch from remote, ignore racing
      ios = CopyAcrossFS(m_local, m_remote, fname, dbg);
    }
  }
  return ios;
}

IOStatus CatFileSystem::GetChildren(const std::string& dir,
                                    const IOOptions& options,
                                    std::vector<std::string>* result,
                                    IODebugContext* dbg) {
  result->clear();
  std::vector<std::string> local, remote;
  IOStatus ios = m_local->GetChildren(dir, options, &local, dbg);
  if (ios.ok()) {
    ios = m_remote->GetChildren(dir, options, &remote, dbg);
  }
  if (ios.ok()) {
    std::sort(local.begin(), local.end());
    std::sort(remote.begin(), remote.end());
    std::set_union(local.begin(), local.end(),
                   remote.begin(), remote.end(),
                   std::back_inserter(*result));
  }
  return ios;
}

IOStatus CatFileSystem::GetChildrenFileAttributes(
    const std::string& dir, const IOOptions& options,
    std::vector<FileAttributes>* result, IODebugContext* dbg) {
  result->clear();
  std::vector<FileAttributes> local, remote;
  IOStatus ios = m_local->GetChildrenFileAttributes(dir, options, &local, dbg);
  if (ios.ok()) {
    ios = m_remote->GetChildrenFileAttributes(dir, options, &remote, dbg);
  }
  if (ios.ok()) {
    auto cmp = TERARK_CMP(name, <);
    std::sort(local.begin(), local.end(), cmp);
    std::sort(remote.begin(), remote.end(), cmp);
    std::set_union(local.begin(), local.end(),
                   remote.begin(), remote.end(),
                   std::back_inserter(*result), cmp);
  }
  return ios;
}

IOStatus CatFileSystem::DeleteFile(const std::string& fname,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  IOStatus ios = m_local->DeleteFile(fname, options, dbg);
  if (ios.ok()) {
    ios = m_remote->DeleteFile(fname, options, dbg);
  }
  return ios;
}

IOStatus CatFileSystem::CreateDir(const std::string& dirname,
                                  const IOOptions& options,
                                  IODebugContext* dbg) {
  IOStatus ios = m_local->CreateDir(dirname, options, dbg);
  if (ios.ok()) {
    ios = m_remote->CreateDir(dirname, options, dbg);
  }
  return ios;
}

IOStatus CatFileSystem::CreateDirIfMissing(const std::string& dirname,
                                           const IOOptions& options,
                                           IODebugContext* dbg) {
  IOStatus ios = m_local->CreateDirIfMissing(dirname, options, dbg);
  if (ios.ok()) {
    ios = m_remote->CreateDirIfMissing(dirname, options, dbg);
  }
  return ios;
}

IOStatus CatFileSystem::DeleteDir(const std::string& dirname,
                                  const IOOptions& options,
                                  IODebugContext* dbg) {
  IOStatus ios = m_local->DeleteDir(dirname, options, dbg);
  if (ios.ok()) {
    ios = m_remote->DeleteDir(dirname, options, dbg);
  }
  return ios;
}

IOStatus CatFileSystem::GetFileSize(const std::string& fname,
                                      const IOOptions& options,
                                      uint64_t* file_size,
                                      IODebugContext* dbg) {
  return m_local->GetFileSize(fname, options, file_size, dbg);
}

IOStatus CatFileSystem::GetFileModificationTime(const std::string& fname,
                                                  const IOOptions& options,
                                                  uint64_t* file_mtime,
                                                  IODebugContext* dbg) {
  return m_local->GetFileSize(fname, options, file_mtime, dbg);
}

IOStatus CatFileSystem::IsDirectory(const std::string& path,
                                    const IOOptions& options, bool* is_dir,
                                    IODebugContext* dbg) {
  return m_local->IsDirectory(path, options, is_dir, dbg);
}

IOStatus CatFileSystem::RenameFile(const std::string& src,
                                   const std::string& dest,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  IOStatus ios1 = m_local->RenameFile(src, dest, options, dbg);
  IOStatus ios2 = m_remote->RenameFile(src, dest, options, dbg);
  if (ios1.ok() && !ios2.ok()) {
    ios2 = CopyAcrossFS(m_remote, m_local, dest, dbg);
  }
  if (ios2.ok() && !ios1.ok()) {
    ios1 = CopyAcrossFS(m_local, m_remote, dest, dbg);
  }
  return ios1.ok() ? ios2 : ios1;
}

IOStatus CatFileSystem::LinkFile(const std::string& src,
                                 const std::string& dest,
                                 const IOOptions& options,
                                 IODebugContext* dbg) {
  IOStatus ios1 = m_local->LinkFile(src, dest, options, dbg);
  IOStatus ios2 = m_remote->LinkFile(src, dest, options, dbg);
  if (ios1.ok() && !ios2.ok()) {
    ios2 = CopyAcrossFS(m_remote, m_local, dest, dbg);
  }
  if (ios2.ok() && !ios1.ok()) {
    ios1 = CopyAcrossFS(m_local, m_remote, dest, dbg);
  }
  return ios1.ok() ? ios2 : ios1;
}

IOStatus CatFileSystem::LockFile(const std::string& fname,
                                 const IOOptions& options, FileLock** lock,
                                 IODebugContext* dbg) {
  return m_local->LockFile(fname, options, lock, dbg);
}

struct CatLogger : public Logger {
  using Logger::Logger;
  std::shared_ptr<Logger> m_local, m_remote;

  ~CatLogger() = default;

  void LogHeader(const char* format, va_list ap) override {
    m_local->LogHeader(format, ap);
    m_remote->LogHeader(format, ap);
  }

  void Logv(const char* format, va_list ap) override {
    m_local->Logv(format, ap);
    m_remote->Logv(format, ap);
  }

  void Logv(const InfoLogLevel log_level, const char* format,
                    va_list ap) override {
    m_local->Logv(log_level, format, ap);
    m_remote->Logv(log_level, format, ap);
  }

  size_t GetLogFileSize() const override { return m_local->GetLogFileSize(); }
  void Flush() {
    m_local->Flush();
    // do not: m_remote->Flush();
  }

 protected:
  Status CloseImpl() override {
    Status s1 = m_local->Close();
    Status s2 = m_remote->Close();
    return s1.ok() ? s2 : s1;
  }
};
IOStatus CatFileSystem::NewLogger(const std::string& fname,
                                  const IOOptions& options,
                                  std::shared_ptr<Logger>* result,
                                  IODebugContext* dbg) {
  auto log = std::make_shared<CatLogger>();
  IOStatus ios = m_local->NewLogger(fname, options, &log->m_local, dbg);
  if (ios.ok()) {
    ios = m_remote->NewLogger(fname, options, &log->m_remote, dbg);
  }
  if (ios.ok()) {
    *result = std::move(log);
  }
  return ios;
}

IOStatus CatFileSystem::GetAbsolutePath(const std::string& db_path,
                                        const IOOptions& options,
                                        std::string* output_path,
                                        IODebugContext* dbg) {
  return m_local->GetAbsolutePath(db_path, options, output_path, dbg);
}


IOStatus CatFileSystem::UnlockFile(FileLock* flock,
                                    const IOOptions& options,
                                    IODebugContext* dbg) {
  return m_local->UnlockFile(flock, options, dbg);
}

IOStatus CatFileSystem::GetTestDirectory(const IOOptions& options,
                                          std::string* path,
                                          IODebugContext* dbg) {
  // copy from ChrootFileSystem::GetTestDirectory
  char buf[256];
  snprintf(buf, sizeof(buf), "/rocksdbtest-%d", static_cast<int>(geteuid()));
  *path = buf;
  return CreateDirIfMissing(*path, options, dbg);
}

}  // namespace ROCKSDB_NAMESPACE
