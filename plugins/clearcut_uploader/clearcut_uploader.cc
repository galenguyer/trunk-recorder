#include <curl/curl.h>
#include <iomanip>
#include <time.h>
#include <vector>

#include "../../trunk-recorder/call_concluder/call_concluder.h"
#include "../../trunk-recorder/plugin_manager/plugin_api.h"
#include "../trunk-recorder/gr_blocks/decoder_wrapper.h"
#include <boost/dll/alias.hpp> // for BOOST_DLL_ALIAS
#include <boost/foreach.hpp>
#include <boost/thread/mutex.hpp>
#include <sys/stat.h>

struct Clearcut_System_Key {
  std::string api_key;
  std::string short_name;
};

struct Clearcut_Uploader_Data {
  std::vector<Clearcut_System_Key> keys;
  std::string Clearcut_server;
};

boost::mutex curl_share_mutex;

class Clearcut_Uploader : public Plugin_Api {
  Clearcut_Uploader_Data data;
  CURLSH *curl_share;
  long curl_dns_ttl;
  std::string plugin_name;

public:
  std::string get_api_key(std::string short_name) {
    for (std::vector<Clearcut_System_Key>::iterator it = data.keys.begin(); it != data.keys.end(); ++it) {
      Clearcut_System_Key key = *it;
      if (key.short_name == short_name) {
        return key.api_key;
      }
    }
    return "";
  }
  static size_t write_callback(void *contents, size_t size, size_t nmemb, void *userp) {
    ((std::string *)userp)->append((char *)contents, size * nmemb);
    return size * nmemb;
  }
  int upload(Call_Data_t call_info) {

    std::string api_key = get_api_key(call_info.short_name);
    if (api_key.size() == 0) {
      return 0;
    }

    std::string loghdr = log_header(call_info.short_name, call_info.call_num, call_info.talkgroup_display, call_info.freq);

    std::ostringstream freq;
    std::string freq_string;
    freq << std::fixed << std::setprecision(0);
    freq << call_info.freq;

    std::ostringstream error_count;
    std::string error_count_string;
    error_count << std::fixed << std::setprecision(0);
    error_count << call_info.error_count;

    std::ostringstream spike_count;
    std::string spike_count_string;
    spike_count << std::fixed << std::setprecision(0);
    spike_count << call_info.spike_count;

    std::ostringstream call_length;
    std::string call_length_string;
    call_length << std::fixed << std::setprecision(0);
    call_length << call_info.length;

    json source_list = json::array();
    for (const auto &source : call_info.transmission_source_list) {
      json entry = {
          {"pos", source.position},
          {"src", source.source},
          {"time", source.time},
      };
      if (!source.tag_ota.empty()) {
        entry["otaAlias"] = source.tag_ota;
        BOOST_LOG_TRIVIAL(debug) << loghdr << this->plugin_name << " otaAlias: '" << source.tag_ota << "' for src " << source.source;
      }
      source_list.push_back(entry);
    }
    std::string source_list_string = source_list.dump();

    freq_string = freq.str();
    error_count_string = error_count.str();
    spike_count_string = spike_count.str();
    call_length_string = call_length.str();

    CURL *curl = curl_easy_init();
    if (!curl) {
      BOOST_LOG_TRIVIAL(error) << loghdr << this->plugin_name << " Upload Error: unable to initialize curl";
      return 1;
    }

    curl_mime *mime = curl_mime_init(curl);
    if (!mime) {
      BOOST_LOG_TRIVIAL(error) << loghdr << this->plugin_name << " Upload Error: unable to initialize mime";
      curl_easy_cleanup(curl);
      return 1;
    }

    curl_mimepart *part;

    /* Fill in the file upload field. This makes libcurl load data from
     the given file name when curl_easy_perform() is called. */
    part = curl_mime_addpart(mime);
    curl_mime_filedata(part, call_info.converted.c_str());
    curl_mime_type(part, "application/octet-stream");
    curl_mime_name(part, "call");

    part = curl_mime_addpart(mime);
    curl_mime_data(part, freq_string.c_str(), CURL_ZERO_TERMINATED);
    curl_mime_name(part, "freq");

    part = curl_mime_addpart(mime);
    curl_mime_data(part, error_count_string.c_str(), CURL_ZERO_TERMINATED);
    curl_mime_name(part, "error_count");

    part = curl_mime_addpart(mime);
    curl_mime_data(part, spike_count_string.c_str(), CURL_ZERO_TERMINATED);
    curl_mime_name(part, "spike_count");

    part = curl_mime_addpart(mime);
    curl_mime_data(part, boost::lexical_cast<std::string>(call_info.start_time).c_str(), CURL_ZERO_TERMINATED);
    curl_mime_name(part, "start_time");

    part = curl_mime_addpart(mime);
    curl_mime_data(part, boost::lexical_cast<std::string>(call_info.stop_time).c_str(), CURL_ZERO_TERMINATED);
    curl_mime_name(part, "stop_time");

    part = curl_mime_addpart(mime);
    curl_mime_data(part, call_length_string.c_str(), CURL_ZERO_TERMINATED);
    curl_mime_name(part, "call_length");

    part = curl_mime_addpart(mime);
    curl_mime_data(part, boost::lexical_cast<std::string>(call_info.talkgroup).c_str(), CURL_ZERO_TERMINATED);
    curl_mime_name(part, "talkgroup_num");

    part = curl_mime_addpart(mime);
    curl_mime_data(part, boost::lexical_cast<std::string>(call_info.emergency).c_str(), CURL_ZERO_TERMINATED);
    curl_mime_name(part, "emergency");

    part = curl_mime_addpart(mime);
    curl_mime_data(part, api_key.c_str(), CURL_ZERO_TERMINATED);
    curl_mime_name(part, "api_key");

    part = curl_mime_addpart(mime);
    curl_mime_data(part, source_list_string.c_str(), CURL_ZERO_TERMINATED);
    curl_mime_name(part, "source_list");

    CURLM *multi_handle = curl_multi_init();
    if (!multi_handle) {
      BOOST_LOG_TRIVIAL(error) << loghdr << this->plugin_name << " Upload Error: unable to initialize curl multi handle";
      curl_mime_free(mime);
      curl_easy_cleanup(curl);
      return 1;
    }

    /* initialize custom header list (stating that Expect: 100-continue is not wanted */
    struct curl_slist *headerlist = curl_slist_append(NULL, "Expect:");

    int still_running = 0;
    std::string response_buffer;
    CURLMcode res;

    std::string url = data.Clearcut_server + "/api/v1/upload";

    /* what URL that receives this POST */
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

    curl_easy_setopt(curl, CURLOPT_USERAGENT, "TrunkRecorder1.0");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headerlist);
    curl_easy_setopt(curl, CURLOPT_MIMEPOST, mime);

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_buffer);

    curl_easy_setopt(curl, CURLOPT_SHARE, curl_share);
    curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT, curl_dns_ttl);

    curl_multi_add_handle(multi_handle, curl);

    curl_multi_perform(multi_handle, &still_running);

    while (still_running) {
      struct timeval timeout;
      int rc;       /* select() return code */
      CURLMcode mc; /* curl_multi_fdset() return code */

      fd_set fdread;
      fd_set fdwrite;
      fd_set fdexcep;
      int maxfd = -1;

      long curl_timeo = -1;

      FD_ZERO(&fdread);
      FD_ZERO(&fdwrite);
      FD_ZERO(&fdexcep);

      /* set a suitable timeout to play around with */
      timeout.tv_sec = 1;
      timeout.tv_usec = 0;

      curl_multi_timeout(multi_handle, &curl_timeo);
      if (curl_timeo >= 0) {
        timeout.tv_sec = curl_timeo / 1000;
        if (timeout.tv_sec > 1)
          timeout.tv_sec = 1;
        else
          timeout.tv_usec = (curl_timeo % 1000) * 1000;
      }

      /* get file descriptors from the transfers */
      mc = curl_multi_fdset(multi_handle, &fdread, &fdwrite, &fdexcep, &maxfd);

      if (mc != CURLM_OK) {
        fprintf(stderr, "curl_multi_fdset() failed, code %d.\n", mc);
        break;
      }

      /* On success the value of maxfd is guaranteed to be >= -1. We call
       select(maxfd + 1, ...); specially in case of (maxfd == -1) there are
       no fds ready yet so we call select(0, ...) --or Sleep() on Windows--
       to sleep 100ms, which is the minimum suggested value in the
       curl_multi_fdset() doc. */

      if (maxfd == -1) {
        /* Portable sleep for platforms other than Windows. */
        struct timeval wait = {0, 100 * 1000}; /* 100ms */
        rc = select(0, NULL, NULL, NULL, &wait);
      } else {
        /* Note that on some platforms 'timeout' may be modified by select().
         If you need access to the original value save a copy beforehand. */
        rc = select(maxfd + 1, &fdread, &fdwrite, &fdexcep, &timeout);
      }

      switch (rc) {
      case -1:
        /* select error */
        break;
      case 0:
      default:
        /* timeout or readable/writable sockets */
        curl_multi_perform(multi_handle, &still_running);
        break;
      }
    }

    long response_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

    res = curl_multi_cleanup(multi_handle);

    /* always cleanup */
    curl_easy_cleanup(curl);

    /* then cleanup the mime */
    curl_mime_free(mime);

    /* free slist */
    curl_slist_free_all(headerlist);

    if (res == CURLM_OK && response_code == 200) {
      struct stat file_info = {};
      stat(call_info.converted.c_str(), &file_info);
      BOOST_LOG_TRIVIAL(info) << loghdr << this->plugin_name << " Upload Success - file size: " << file_info.st_size;
      return 0;
    }

    BOOST_LOG_TRIVIAL(error) << loghdr << this->plugin_name << " Upload Error: " << response_buffer;
    return 1;
  }

  int call_end(Call_Data_t call_info) {
    return upload(call_info);
  }

  int parse_config(json config_data) {
    // Extract plugin name from config, with fallback for default internal plugin name
    std::string config_name = config_data.value("name", "clearcut_uploader");
    this->plugin_name = (config_name == "clearcut_uploader") ? "ClearCut" : config_name;
    std::string log_prefix = "\t[ClearCut]\t";

    // Tests to see if the clearcutServer value exists in the config file
    bool upload_server_exists = config_data.contains("clearcutServer");
    if (!upload_server_exists) {
      return 1;
    }

    this->data.Clearcut_server = config_data.value("clearcutServer", "");
    BOOST_LOG_TRIVIAL(info) << log_prefix << "ClearCut Server: " << this->data.Clearcut_server;

    // from: http://www.zedwood.com/article/cpp-boost-url-regex
    boost::regex api_regex("(.*)(.{2}$)");
    boost::regex ex("(http|https)://([^/ :]+):?([^/ ]*)(/?[^ #?]*)\\x3f?([^ #]*)#?([^ ]*)");
    boost::cmatch what;

    if (!regex_match(this->data.Clearcut_server.c_str(), what, ex)) {
      BOOST_LOG_TRIVIAL(error) << log_prefix << "Unable to parse Server URL\n";
      return 1;
    }
    // Gets the API key for each system, if defined
    for (json element : config_data["systems"]) {
      bool clearcut_exists = element.contains("clearcutApiKey");
      if (clearcut_exists) {
        Clearcut_System_Key key;
        key.api_key = element.value("clearcutApiKey", "");
        key.short_name = element.value("shortName", "");
        regex_match(key.api_key.c_str(), what, api_regex);
        std::string redacted_api(what[2].first, what[2].second);
        BOOST_LOG_TRIVIAL(info) << log_prefix << "Uploading calls for: " << key.short_name << "\t API Key: ******" << redacted_api;
        this->data.keys.push_back(key);
      }
    }

    if (this->data.keys.size() == 0) {
      BOOST_LOG_TRIVIAL(error) << log_prefix << "Clearcut Server set, but no Systems are configured\n";
      return 1;
    }

    // Initialize shared curl cache to reduce DNS lookups (5 min TTL)
    curl_share = curl_share_init();
    curl_share_setopt(curl_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
    curl_share_setopt(curl_share, CURLSHOPT_LOCKFUNC, curl_lock_cb);
    curl_share_setopt(curl_share, CURLSHOPT_UNLOCKFUNC, curl_unlock_cb);
    curl_dns_ttl = 300;

    return 0;
  }

  // curl callbacks
  static void curl_lock_cb(CURL *handle, curl_lock_data data, curl_lock_access access, void *userptr) {
    curl_share_mutex.lock();
  }

  static void curl_unlock_cb(CURL *handle, curl_lock_data data, curl_lock_access access, void *userptr) {
    curl_share_mutex.unlock();
  }

  /*
 int init(Config *config, std::vector<Source *> sources, std::vector<System *> systems) { return 0; }
   int start() { return 0; }
   int stop() { return 0; }
   int poll_one() { return 0; }
   int signal(long unitId, const char *signaling_type, gr::blocks::SignalType sig_type, Call *call, System *system, Recorder *recorder) { return 0; }
   int audio_stream(Recorder *recorder, float *samples, int sampleCount) { return 0; }
   int call_start(Call *call) { return 0; }
   int calls_active(std::vector<Call *> calls) { return 0; }
   int setup_recorder(Recorder *recorder) { return 0; }
   int setup_system(System *system) { return 0; }
   int setup_systems(std::vector<System *> systems) { return 0; }
   int setup_sources(std::vector<Source *> sources) { return 0; }
   int setup_config(std::vector<Source *> sources, std::vector<System *> systems) { return 0; }
   int system_rates(std::vector<System *> systems, float timeDiff) { return 0; }
*/
  // Factory method
  static boost::shared_ptr<Clearcut_Uploader> create() {
    return boost::shared_ptr<Clearcut_Uploader>(
        new Clearcut_Uploader());
  }
};

BOOST_DLL_ALIAS(
    Clearcut_Uploader::create, // <-- this function is exported with...
    create_plugin             // <-- ...this alias name
)
