//  Copyright (c) Topling, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <rocksdb/file_system.h>

namespace ROCKSDB_NAMESPACE {

// Like EnvMirror.
// 1. Read local, write both, if not write through, do write on close.
// 2. In most cases, local act as a cache for remote.
// 3. local may be ChrootFileSystem, may be read mmap
class CatFileSystem : public FileSystem {
 public:
  CatFileSystem(const std::shared_ptr<FileSystem>& local,
                const std::shared_ptr<FileSystem>& remote,
                bool use_mmap_read);

  const char* Name() const override { return "CatFileSystem"; }
  static const char* kClassName() { return "CatFileSystem"; }
  bool IsInstanceOf(const std::string& id) const override;

  Status RegisterDbPaths(const std::vector<std::string>& paths) override;
  Status UnregisterDbPaths(const std::vector<std::string>& paths) override;

  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override;

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override;

  IOStatus NewWritableFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override;

  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& options,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override;

  IOStatus NewRandomRWFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override;

  IOStatus NewDirectory(const std::string& dir, const IOOptions& options,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;

  IOStatus FileExists(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override;

  IOStatus GetChildrenFileAttributes(const std::string& dir,
                                     const IOOptions& options,
                                     std::vector<FileAttributes>* result,
                                     IODebugContext* dbg) override;

  IOStatus DeleteFile(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;

  IOStatus CreateDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus CreateDirIfMissing(const std::string& dirname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;

  IOStatus DeleteDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override;

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override;

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override;

  IOStatus IsDirectory(const std::string& path, const IOOptions& options,
                       bool* is_dir, IODebugContext* dbg) override;

  IOStatus RenameFile(const std::string& src, const std::string& dest,
                      const IOOptions& options, IODebugContext* dbg) override;

  IOStatus LinkFile(const std::string& src, const std::string& dest,
                    const IOOptions& options, IODebugContext* dbg) override;

  IOStatus LockFile(const std::string& fname, const IOOptions& options,
                    FileLock** lock, IODebugContext* dbg) override;

  IOStatus NewLogger(const std::string& fname, const IOOptions& options,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override;

  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override;

  IOStatus GetTestDirectory(const IOOptions&, std::string* path, IODebugContext*);
  IOStatus UnlockFile(FileLock*, const IOOptions&, IODebugContext*);

private:
  std::shared_ptr<FileSystem> m_local, m_remote;
  bool m_use_mmap_reads; // override FileOptions::use_mmap_reads
};

}  // namespace ROCKSDB_NAMESPACE

