//
// Created by leipeng on 2020/8/18.
//

/* Copyright (c) 2013-2018 the Civetweb developers
 * Copyright (c) 2013 No Face Press, LLC
 * License http://opensource.org/licenses/mit-license.php MIT License
 */

// Simple example program on how to use Embedded C++ interface.

#include "CivetServer.h"
#include <cstring>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

#include <utilities/json/json_plugin_factory.h>
#include "json_civetweb.h"

#define DOCUMENT_ROOT "."
#define PORT "8081"
#define EXAMPLE_URI "/example"
#define EXIT_URI "/exit"


namespace ROCKSDB_NAMESPACE {

/* Exit flag for main loop */
volatile bool exitNow = false;

json from_query_string(const char* qry) {
  json js;
  if (!qry)
    return js;
  const char* end = qry + strlen(qry);
  while (qry < end) {
    const char* sep = std::find(qry, end, '&');
    const char* eq = std::find(qry, sep, '=');
    std::string name(qry, eq);
    auto& value_ref = js[name];
    std::string value;
    if (eq != sep)
      value.assign(eq+1, sep);
    if (value_ref.is_null())
      value_ref = std::move(value);
    else if (value_ref.is_string()){
      value_ref = json::array({value_ref, value});
    }
    else if (value_ref.is_array()) {
      value_ref.push_back(value);
    }
    else {
      ROCKSDB_DIE("should not goes here");
    }
    qry = sep + 1;
  }
  return js;
}

static time_t g_web_start_time = ::time(NULL); // NOLINT

std::string cur_time_stat() {
  char buf[64];
  time_t rawtime;
  time(&rawtime);
  struct tm  result;
  struct tm* timeinfo = localtime_r(&rawtime, &result);
  strftime(buf, sizeof(buf), "%F %T", timeinfo);
  size_t sec = (size_t)difftime(rawtime, g_web_start_time);
  size_t days = sec / 86400; sec %= 86400;
  size_t hours = sec / 3600; sec %= 3600;
  size_t minites = sec / 60; sec %= 60;
  std::string str; str.resize(256);
  str.resize(snprintf(&str[0], str.size(), "%s , Up: %zd-%02zd:%02zd:%02zd",
                      buf, days, hours, minites, sec));
  return str;
}
void mg_print_cur_time(mg_connection *conn) {
  std::string str = cur_time_stat();
  mg_printf(conn, "<p>%s</p>\r\n", cur_time_stat().c_str());
}

template<class Ptr>
class RepoHandler : public CivetHandler {
public:
  JsonPluginRepo* m_repo;
  JsonPluginRepo::Impl::ObjMap<Ptr>* m_map;
  Slice m_ns;
  //std::string m_clazz;

  RepoHandler(const char* clazz,
              JsonPluginRepo* repo,
              JsonPluginRepo::Impl::ObjMap<Ptr>* map) {
    m_repo = repo;
    //m_clazz = clazz;
    m_ns = clazz;
    m_map = map;
    if (JsonPluginRepo::DebugLevel() >= 2) {
      fprintf(stderr, "INFO: http: clazz: %s\n", clazz);
    }
  }

  bool handleGet(CivetServer *server, struct mg_connection *conn) override {
    mg_printf(conn,
              "HTTP/1.1 200 OK\r\nContent-Type: "
              "text/html\r\nConnection: close\r\n\r\n");

    const mg_request_info* req = mg_get_request_info(conn);
    json query = from_query_string(req->query_string);
//    if (JsonPluginRepo::DebugLevel() >= 2) {
//      fprintf(stderr, "INFO: query = %s\n", query.dump().c_str());
//    }
    const char* uri = req->local_uri;
    if (nullptr == uri) {
      mg_printf(conn, "ERROR: local uri is null\r\n");
      return true;
    }
    const bool html = JsonSmartBool(query, "html");
    while ('/' == *uri) uri++;
    size_t urilen = strlen(uri);
//    if (urilen < m_ns.size()) {
//      mg_printf(conn, "ERROR: local uri is too short = %zd\r\n", urilen);
//      return true;
//    }
//    if (memcmp(uri, m_ns.data(), m_ns.size()) != 0) {
//      mg_printf(conn, "ERROR: registered uri = %s, request uri = %s\r\n", m_ns.data(), uri);
//      return true;
//    }
    auto slash = (const char*)memchr(uri, '/', urilen);
    if (NULL == slash) {
      std::vector<std::pair<std::string, Ptr> > vec;
      vec.reserve(m_map->name2p->size());
      vec.assign(m_map->name2p->begin(), m_map->name2p->end());
      std::sort(vec.begin(), vec.end());
      if (!html) {
        json djs;
        for (auto& x : vec) {
          djs.push_back(x.first);
        }
        std::string jstr = djs.dump();
        mg_write(conn, jstr.data(), jstr.size());
      }
      else {
        mg_printf(conn, "<html><title>db list</title>\n<body>\n");
        mg_print_cur_time(conn);
        mg_printf(conn, "<table border=1><tbody>\n");
        for (auto& kv : vec) {
          mg_printf(conn, "<tr><td><a href='/%.*s/%s?html=1'>%s</a></td></tr>\n",
                    int(urilen), uri, kv.first.c_str(), kv.first.c_str());
        }
        mg_printf(conn, "</tbody></table></body></html>\n");
      }
      return true;
    }
    const char* name = slash + 1;
    auto iter = m_map->name2p->find(name);
    if (m_map->name2p->end() != iter) {
      auto& p = iter->second;
      if (html) {
        mg_printf(conn, "<html><title>%s</title><body>\n", name);
        mg_print_cur_time(conn);
      }
      try {
        std::string str = PluginToString(p, *m_map, query, *m_repo);
        mg_write(conn, str.data(), str.size());
      }
      catch (const Status& es) {
        mg_printf(conn, "Caught Status: %s\n", es.ToString().c_str());
      }
      catch (const std::exception& ex) {
        mg_printf(conn, "Caught std::exception: %s\n", ex.what());
      }
      if (html)
        mg_printf(conn, "</body></html>\n");
    }
    else if (html) {
      mg_printf(conn, "<html><title>ERROR</title><body>\r\n");
      mg_printf(conn, "<h1>ERROR: not found: %s</h1>\r\n", uri);
      mg_printf(conn, "<h1><a href='/%.*s'>see all %.*s</a>\r\n",
                int(slash - uri), uri,
                int(slash - uri), uri);
      mg_printf(conn, "</body></html>\r\n");
    }
    else {
      mg_printf(conn, R"({status:"NotFound", namespace:"%s", objname:"%s"})",
                m_ns.data_, name);
    }
    return true;
  }
};

template<class Ptr>
RepoHandler<Ptr>*
NewRepoHandler(const char* clazz, JsonPluginRepo* repo,
               JsonPluginRepo::Impl::ObjMap<Ptr>* map) {
  return new RepoHandler<Ptr>(clazz, repo, map);
}

#define ADD_HANDLER(clazz, varname) do { \
  auto p = NewRepoHandler(#clazz, repo, &repo->m_impl->varname); \
  m_server->addHandler("/" #clazz, *p);  \
  m_server->addHandler("/" #varname, *p);  \
  m_clean.push_back([p](){ delete p; }); \
} while (0)                             \

class JsonCivetServer::Impl {
public:
  std::unique_ptr<CivetServer> m_server;
  std::vector<std::function<void()> > m_clean;

  Impl(const json& conf, JsonPluginRepo* repo);
  ~Impl() {
    for (auto& clean: m_clean) {
      clean();
    }
  }
};

JsonCivetServer::Impl::Impl(const json& conf, JsonPluginRepo* repo) {
  mg_init_library(0);
  if (!conf.is_object()) {
    THROW_InvalidArgument(
        "conf must be a json object, but is: " + conf.dump());
  }
  std::vector<std::string> options;
  for (const auto& kv : conf.items()) {
    std::string key = kv.key();
    const auto& value = kv.value();
    if (!value.is_string()) {
      THROW_InvalidArgument(
        "conf[\"" + key + "\"] must be a string, but is: " + value.dump());
    }
    options.push_back(std::move(key));
    options.push_back(value.get_ref<const std::string&>());
  }
  if (JsonPluginRepo::DebugLevel() >= 2) {
    for (const auto& val : options) {
      fprintf(stderr, "INFO: JsonCivetServer::Impl::Impl(): len=%02zd: %s\n", val.size(), val.c_str());
    }
  }
  m_server.reset(new CivetServer(options));

  ADD_HANDLER(AnyPlugin, any_plugin);
  ADD_HANDLER(Cache, cache);
  ADD_HANDLER(PersistentCache, persistent_cache);
  ADD_HANDLER(CompactionFilterFactory, compaction_filter_factory);
  ADD_HANDLER(Comparator, comparator);
  ADD_HANDLER(ConcurrentTaskLimiter, compaction_thread_limiter);
  ADD_HANDLER(Env, env);
  ADD_HANDLER(EventListener, event_listener);
  ADD_HANDLER(FileChecksumGenFactory, file_checksum_gen_factory);
  ADD_HANDLER(FileSystem, file_system);
  ADD_HANDLER(FilterPolicy, filter_policy);
  ADD_HANDLER(FlushBlockPolicyFactory, flush_block_policy_factory);
  ADD_HANDLER(Logger, info_log);
  ADD_HANDLER(MemoryAllocator, memory_allocator);
  ADD_HANDLER(MemTableRepFactory, memtable_factory);
  ADD_HANDLER(MergeOperator, merge_operator);
  ADD_HANDLER(RateLimiter, rate_limiter);
  ADD_HANDLER(SstFileManager, sst_file_manager);
  ADD_HANDLER(Statistics, statistics);
  ADD_HANDLER(TableFactory, table_factory);
  ADD_HANDLER(TablePropertiesCollectorFactory, table_properties_collector_factory);
  ADD_HANDLER(TransactionDBMutexFactory, txn_db_mutex_factory);
  ADD_HANDLER(SliceTransform, slice_transform);
  ADD_HANDLER(SstPartitionerFactory, sst_partitioner_factory);
  ADD_HANDLER(CompactionExecutorFactory, compaction_executor_factory);

  ADD_HANDLER(Options, options);
  ADD_HANDLER(DBOptions, db_options);
  ADD_HANDLER(CFOptions, cf_options);

  ADD_HANDLER(CFPropertiesWebView, props);

  //using DataBase = DB_Ptr;
  ADD_HANDLER(DataBase, db);
}

void JsonCivetServer::Init(const json& conf, JsonPluginRepo* repo) {
  if (!m_impl)
    m_impl = new Impl(conf, repo);
}
void JsonCivetServer::Close() {
  delete m_impl;
  m_impl = nullptr;
}
JsonCivetServer::JsonCivetServer() {
  m_impl = nullptr;
}
JsonCivetServer::~JsonCivetServer() {
  delete m_impl;
  mg_exit_library();
}

} // ROCKSDB_NAMESPACE
