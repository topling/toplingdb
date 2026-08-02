/*
 * sample_statm_fdcache <series_file> <time_file> <command> [args...]
 *
 * Copied from sample_statm.c; adds last column pagecache.
 *
 * Run command; once per second append to series_file:
 *   <epoch> <statm fields...> <pagecache>
 * where <statm fields...> is the full /proc/<pid>/statm line
 * (size resident shared text lib data dt), in pages, and <pagecache>
 * is page-cache residency (pages) of unique regular files the child
 * currently has open (by /proc/<pid>/fd, deduped by dev:ino; not
 * distinguishing whether those pages are mmap'd).
 *
 * Also write GNU-time-compatible max RSS to time_file:
 *   max_rss_kb=<kilobytes>
 *
 * Child inherits stdio; redirect outside if you want a log.
 * Callers extract resident (field 2) or any other column as needed.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/mman.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <functional>
#include <unordered_set>
#include <utility>

#ifndef __NR_cachestat
#error "cachestat(__NR_cachestat) missing from system headers"
#endif

static int g_series_fd = -1;
static int g_statm_fd = -1;
static pid_t g_child = -1;
static long g_page_size = 4096;
static volatile sig_atomic_t g_alrm;
static int g_cachestat_fail_logged;

using DevIno = std::pair<dev_t, ino_t>;

struct DevInoHash {
  size_t operator()(const DevIno &x) const noexcept {
    return std::hash<uint64_t>{}(static_cast<uint64_t>(x.first)) ^
           (std::hash<uint64_t>{}(static_cast<uint64_t>(x.second)) << 1);
  }
};

static void on_alrm(int signo) {
  (void)signo;
  g_alrm = 1;
}

/* Probe cachestat(2); returns a short status token for the series header. */
static const char *probe_cachestat(int fd) {
  struct cachestat_range range = {};
  struct cachestat cs;
  if (syscall(__NR_cachestat, fd, &range, &cs, 0) == 0) {
    return "ok";
  }
  if (errno == ENOSYS) {
    return "enosys";
  }
  if (errno == EPERM) {
    return "eperm";
  }
  if (errno == EOPNOTSUPP) {
    return "eopnotsupp";
  }
  return "error";
}

/* cachestat(2): len==0 means whole file. Returns 0 on failure. */
static long file_cached_pages(int fd) {
  struct cachestat_range range = {};
  struct cachestat cs;
  if (syscall(__NR_cachestat, fd, &range, &cs, 0) != 0) {
    if (!g_cachestat_fail_logged) {
      g_cachestat_fail_logged = 1;
      fprintf(stderr, "sample_statm_fdcache: cachestat failed: %s\n",
              strerror(errno));
    }
    return 0;
  }
  return (long)cs.nr_cache;
}

/* Sum page-cache pages of child's open regular files (dedupe dev:ino). */
static long fd_page_cache_pages(pid_t pid) {
  char dirpath[64];
  int n = snprintf(dirpath, sizeof(dirpath), "/proc/%d/fd", (int)pid);
  if (n < 0 || n >= (int)sizeof(dirpath)) {
    return 0;
  }

  int proc_fd = open(dirpath, O_RDONLY | O_DIRECTORY);
  if (proc_fd < 0) {
    return 0;
  }

  int list_fd = dup(proc_fd);
  if (list_fd < 0) {
    close(proc_fd);
    return 0;
  }

  DIR *dir = fdopendir(list_fd);
  if (!dir) {
    close(list_fd);
    close(proc_fd);
    return 0;
  }

  std::unordered_set<DevIno, DevInoHash> seen;
  long total = 0;

  struct dirent *ent;
  while ((ent = readdir(dir)) != NULL) {
    if (ent->d_name[0] == '.') {
      continue;
    }

    /* Open via proc_fd/ slot (openat) instead of reconstructing full path. */
    int fd = openat(proc_fd, ent->d_name, O_RDONLY);
    if (fd < 0) {
      continue;
    }

    struct stat st;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) {
      close(fd);
      continue;
    }

    if (!seen.insert(DevIno{st.st_dev, st.st_ino}).second) {
      close(fd);
      continue;
    }

    total += file_cached_pages(fd);
    close(fd);
  }

  closedir(dir);
  close(proc_fd);
  return total;
}

static void sample_once(void) {
  char buf[128];
  char out[256];
  struct timespec ts;

  if (g_statm_fd < 0 || g_series_fd < 0 || g_child < 0) {
    return;
  }

  ssize_t n = pread(g_statm_fd, buf, sizeof(buf) - 1, 0);
  if (n <= 0) {
    return;
  }
  while (n > 0 && (buf[n - 1] == '\n' || buf[n - 1] == '\0')) {
    n--;
  }
  buf[n] = '\0';

  long pagecache = fd_page_cache_pages(g_child);

  if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
    return;
  }

  int len = snprintf(out, sizeof(out), "%ld.%06ld %s %ld\n", (long)ts.tv_sec,
                     ts.tv_nsec / 1000L, buf, pagecache);
  if (len > 0 && len < (int)sizeof(out)) {
    ssize_t nw = write(g_series_fd, out, (size_t)len);
    (void)nw;
  }
}

static void stop_timer(void) {
  struct itimerval it = {};
  setitimer(ITIMER_REAL, &it, NULL);
  signal(SIGALRM, SIG_DFL);
  g_alrm = 0;
}

static int write_time_file(const char *path, long max_rss_kb) {
  char line[64];
  int len = snprintf(line, sizeof(line), "max_rss_kb=%ld\n", max_rss_kb);
  if (len < 0 || len >= (int)sizeof(line)) {
    return -1;
  }

  int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    return -1;
  }

  ssize_t n = write(fd, line, (size_t)len);
  close(fd);
  return n == (ssize_t)len ? 0 : -1;
}

int main(int argc, char **argv) {
  if (argc < 4) {
    fprintf(stderr,
            "Usage: %s <series_file> <time_file> <command> [args...]\n",
            argv[0]);
    return 1;
  }

  const char *series_path = argv[1];
  const char *time_path = argv[2];
  int status = 0;

  pid_t child = fork();
  if (child < 0) {
    perror("fork");
    return 1;
  }
  if (child == 0) {
    execvp(argv[3], argv + 3);
    _exit(127);
  }
  g_child = child;

  char statm_path[64];
  snprintf(statm_path, sizeof(statm_path), "/proc/%d/statm", (int)child);
  g_statm_fd = open(statm_path, O_RDONLY);
  if (g_statm_fd < 0) {
    fprintf(stderr, "open %s: %s\n", statm_path, strerror(errno));
    goto wait_fail;
  }

  /* Block scope: C++ forbids goto across non-trivial initializations. */
  {
    g_page_size = sysconf(_SC_PAGESIZE);
    if (g_page_size < 0) {
      g_page_size = 4096;
    }

    struct timespec ts0;
    if (clock_gettime(CLOCK_REALTIME, &ts0) != 0) {
      perror("clock_gettime");
      goto wait_fail;
    }

    g_series_fd = open(series_path, O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644);
    if (g_series_fd < 0) {
      perror("open series");
      goto wait_fail;
    }

    const char *cachestat_st = probe_cachestat(g_series_fd);
    if (strcmp(cachestat_st, "ok") != 0) {
      fprintf(stderr, "sample_statm_fdcache: cachestat probe=%s (%s)\n",
              cachestat_st, strerror(errno));
    }

    char header[224];
    int header_len = snprintf(
        header, sizeof(header),
        "# start_epoch=%ld.%06ld  page_size=%ld  cachestat=%s  "
        "fields=size,resident,shared,text,lib,data,dt,pagecache\n",
        (long)ts0.tv_sec, ts0.tv_nsec / 1000L, g_page_size, cachestat_st);
    if (header_len < 0 || header_len >= (int)sizeof(header) ||
        write(g_series_fd, header, (size_t)header_len) != (ssize_t)header_len) {
      perror("write header");
      goto wait_fail;
    }

    struct sigaction sa = {};
    sa.sa_handler = on_alrm;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGALRM, &sa, NULL);

    struct itimerval it = {};
    it.it_value.tv_sec = 1;
    it.it_interval.tv_sec = 1;
    setitimer(ITIMER_REAL, &it, NULL);

    struct rusage ru;
    while (wait4(child, &status, 0, &ru) < 0) {
      if (errno == EINTR) {
        if (g_alrm) {
          g_alrm = 0;
          sample_once();
        }
        continue;
      }
      perror("wait4");
      stop_timer();
      close(g_statm_fd);
      close(g_series_fd);
      return 1;
    }

    stop_timer();
    close(g_statm_fd);
    close(g_series_fd);
    g_statm_fd = g_series_fd = -1;
    g_child = -1;

    /* Linux: ru_maxrss is kilobytes (same unit as GNU time %M). */
    if (write_time_file(time_path, ru.ru_maxrss) != 0) {
      perror("write time file");
      return 1;
    }

    if (WIFEXITED(status)) {
      return WEXITSTATUS(status);
    }
    if (WIFSIGNALED(status)) {
      return 128 + WTERMSIG(status);
    }
    return 1;
  }

wait_fail:
  while (waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  if (g_statm_fd >= 0) {
    close(g_statm_fd);
  }
  if (g_series_fd >= 0) {
    close(g_series_fd);
  }
  return 1;
}
