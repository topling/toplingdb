/*
 * sample_statm <series_file> <time_file> <command> [args...]
 *
 * Lightweight /proc/<pid>/statm sampler (no open-file page-cache column).
 * CI benches use sample_statm_fdcache; keep this for local/debug without fd_cache.
 *
 * Run command; once per second append to series_file:
 *   <epoch> <statm fields...>
 * where <statm fields...> is the full /proc/<pid>/statm line
 * (size resident shared text lib data dt), in pages.
 *
 * Also write GNU-time-compatible max RSS to time_file:
 *   max_rss_kb=<kilobytes>
 *
 * Child inherits stdio; redirect outside if you want a log.
 * Callers extract resident (field 2) or any other column as needed.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int g_series_fd = -1;
static int g_statm_fd = -1;

static void on_alrm(int signo) {
  char buf[128];
  char out[192];
  struct timespec ts;

  (void)signo;
  if (g_statm_fd < 0 || g_series_fd < 0) {
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

  if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
    return;
  }

  int len = snprintf(out, sizeof(out), "%ld.%06ld %s\n", (long)ts.tv_sec,
                     ts.tv_nsec / 1000L, buf);
  if (len > 0 && len < (int)sizeof(out)) {
    ssize_t nw = write(g_series_fd, out, (size_t)len);
    (void)nw;
  }
}

static void stop_timer(void) {
  struct itimerval it = {0};
  setitimer(ITIMER_REAL, &it, NULL);
  signal(SIGALRM, SIG_DFL);
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

  char statm_path[64];
  snprintf(statm_path, sizeof(statm_path), "/proc/%d/statm", (int)child);
  g_statm_fd = open(statm_path, O_RDONLY);
  if (g_statm_fd < 0) {
    fprintf(stderr, "open %s: %s\n", statm_path, strerror(errno));
    goto wait_fail;
  }

  long page_size = sysconf(_SC_PAGESIZE);
  if (page_size < 0) {
    page_size = 4096;
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

  char header[160];
  int header_len = snprintf(
      header, sizeof(header),
      "# start_epoch=%ld.%06ld  page_size=%ld  "
      "fields=size,resident,shared,text,lib,data,dt\n",
      (long)ts0.tv_sec, ts0.tv_nsec / 1000L, page_size);
  if (header_len < 0 || header_len >= (int)sizeof(header) ||
      write(g_series_fd, header, (size_t)header_len) != (ssize_t)header_len) {
    perror("write header");
    goto wait_fail;
  }

  struct sigaction sa = {.sa_handler = on_alrm};
  sigemptyset(&sa.sa_mask);
  sigaction(SIGALRM, &sa, NULL);

  struct itimerval it = {
      .it_value = {.tv_sec = 1},
      .it_interval = {.tv_sec = 1},
  };
  setitimer(ITIMER_REAL, &it, NULL);

  struct rusage ru;
  while (wait4(child, &status, 0, &ru) < 0) {
    if (errno != EINTR) {
      perror("wait4");
      stop_timer();
      close(g_statm_fd);
      close(g_series_fd);
      return 1;
    }
  }

  stop_timer();
  close(g_statm_fd);
  close(g_series_fd);
  g_statm_fd = g_series_fd = -1;

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
