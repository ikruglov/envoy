#include "common/access_log/grpc_access_log_impl.h"

#include "common/grpc/async_client_impl.h"

namespace Envoy {
namespace AccessLog {

GrpcAccessLogStreamer::GrpcAccessLogStreamer(Upstream::ClusterManager& cluster_manager,
                                             ThreadLocal::SlotAllocator& tls,
                                             const LocalInfo::LocalInfo& local_info,
                                             const std::string& cluster_name)
    : tls_slot_(tls.allocateSlot()) {

  SharedStateSharedPtr shared_state = std::make_shared<SharedState>(local_info);
  tls_slot_->set([shared_state, &cluster_manager, cluster_name](Event::Dispatcher&) {
    return ThreadLocal::ThreadLocalObjectSharedPtr{
        new ThreadLocalStreamer(shared_state, cluster_manager, cluster_name)};
  });
}

GrpcAccessLogStreamer::ThreadLocalStreamer::ThreadLocalStreamer(
    const SharedStateSharedPtr& shared_state, Upstream::ClusterManager& cluster_manager,
    const std::string& cluster_name)
    : client_(
          new Grpc::AsyncClientImpl<envoy::api::v2::filter::accesslog::StreamAccessLogsMessage,
                                    envoy::api::v2::filter::accesslog::StreamAccessLogsResponse>(
              cluster_manager, cluster_name)),
      shared_state_(shared_state) {}

void GrpcAccessLogStreamer::ThreadLocalStreamer::send(
    envoy::api::v2::filter::accesslog::StreamAccessLogsMessage& message,
    const std::string& log_name) {
  auto& stream_entry = stream_map_[log_name];
  if (stream_entry == nullptr) {
    stream_entry =
        client_->start(*Protobuf::DescriptorPool::generated_pool()->FindMethodByName(
                           "envoy.api.v2.filter.accesslog.AccessLogService.StreamAccessLogs"),
                       *this);

    auto* identifier = message.mutable_identifier();
    *identifier->mutable_node() = shared_state_->local_info_.node();
    identifier->set_log_name(log_name);
  }

  if (stream_entry) {
    stream_entry->sendMessage(message, false);
  }
}

void GrpcAccessLogStreamer::ThreadLocalStreamer::onRemoteClose(Grpc::Status::GrpcStatus,
                                                               const std::string&) {
  // fixfix
}

HttpGrpcAccessLog::HttpGrpcAccessLog(
    FilterPtr&& filter, const envoy::api::v2::filter::accesslog::HttpGrpcAccessLogConfig& config,
    GrpcAccessLogStreamerSharedPtr grpc_access_log_streamer)
    : filter_(std::move(filter)), config_(config),
      grpc_access_log_streamer_(grpc_access_log_streamer) {}

void HttpGrpcAccessLog::log(const Http::HeaderMap* request_headers,
                            const Http::HeaderMap* response_headers,
                            const RequestInfo::RequestInfo& request_info) {
  static Http::HeaderMapImpl empty_headers;
  if (!request_headers) {
    request_headers = &empty_headers;
  }
  if (!response_headers) {
    response_headers = &empty_headers;
  }

  if (filter_) {
    if (!filter_->evaluate(request_info, *request_headers)) {
      return;
    }
  }

  envoy::api::v2::filter::accesslog::StreamAccessLogsMessage message;
  auto* log_entry = message.mutable_http_logs()->add_log_entry();

  // Common log properties.
  // TODO(mattklein123): Populate sample_rate field.
  // TODO(mattklein123): Populate tls_properties field.
  // TODO(mattklein123): Populate time_to_first_upstream_rx_byte field.
  // TODO(mattklein123): Populate metadata field and wire up to filters.
  auto* common_properties = log_entry->mutable_common_properties();
  // fixfix address
  common_properties->mutable_start_time()->MergeFrom(
      Protobuf::util::TimeUtil::MicrosecondsToTimestamp(
          std::chrono::duration_cast<std::chrono::microseconds>(
              request_info.startTime().time_since_epoch())
              .count()));
  if (request_info.requestReceivedDuration().valid()) {
    common_properties->mutable_time_to_last_rx_byte()->MergeFrom(
        Protobuf::util::TimeUtil::MicrosecondsToDuration(
            request_info.requestReceivedDuration().value().count()));
  }
  if (request_info.responseReceivedDuration().valid()) {
    common_properties->mutable_time_to_first_upstream_rx_byte()->MergeFrom(
        Protobuf::util::TimeUtil::MicrosecondsToDuration(
            request_info.responseReceivedDuration().value().count()));
  }
  if (request_info.upstreamHost() != nullptr) {
    common_properties->set_upstream_cluster(request_info.upstreamHost()->cluster().name());
  }

  if (request_info.protocol().valid()) {
    switch (request_info.protocol().value()) {
    case Http::Protocol::Http10:
      log_entry->set_protocol_version(
          envoy::api::v2::filter::accesslog::HTTPAccessLogEntry::HTTP10);
      break;
    case Http::Protocol::Http11:
      log_entry->set_protocol_version(
          envoy::api::v2::filter::accesslog::HTTPAccessLogEntry::HTTP11);
      break;
    case Http::Protocol::Http2:
      log_entry->set_protocol_version(envoy::api::v2::filter::accesslog::HTTPAccessLogEntry::HTTP2);
      break;
    }
  }

  // HTTP request properities.
  // TODO(mattklein123): Populate port field.
  // TODO(mattklein123): Populate custom request headers.
  auto* request_properties = log_entry->mutable_request();
  if (request_headers->Scheme() != nullptr) {
    request_properties->set_scheme(request_headers->Scheme()->value().c_str());
  }
  if (request_headers->Host() != nullptr) {
    request_properties->set_authority(request_headers->Host()->value().c_str());
  }
  if (request_headers->Path() != nullptr) {
    request_properties->set_path(request_headers->Path()->value().c_str());
  }
  if (request_headers->UserAgent() != nullptr) {
    request_properties->set_user_agent(request_headers->UserAgent()->value().c_str());
  }
  // fixfix referer
  if (request_headers->ForwardedFor() != nullptr) {
    request_properties->set_forwarded_for(request_headers->ForwardedFor()->value().c_str());
  }
  if (request_headers->RequestId() != nullptr) {
    request_properties->set_request_id(request_headers->RequestId()->value().c_str());
  }
  if (request_headers->EnvoyOriginalPath() != nullptr) {
    request_properties->set_original_path(request_headers->EnvoyOriginalPath()->value().c_str());
  }
  request_properties->set_request_headers_bytes(request_headers->byteSize());
  request_properties->set_request_body_bytes(request_info.bytesReceived());

  // HTTP response properties.
  // TODO(mattklein123): Populate custom response headers.
  auto* response_properties = log_entry->mutable_response();
  if (request_info.responseCode().valid()) {
    response_properties->mutable_response_code()->set_value(request_info.responseCode().value());
  }
  response_properties->set_response_headers_bytes(response_headers->byteSize());
  response_properties->set_response_body_bytes(request_info.bytesSent());

  // TODO(mattklein123): Consider batching multiple logs and flushing.
  grpc_access_log_streamer_->send(message, config_.common_config().log_name());
}

} // namespace AccessLog
} // namespace Envoy
