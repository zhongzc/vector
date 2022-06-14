use std::sync::Arc;

use futures::SinkExt;
use protobuf::Message;

#[derive(Clone)]
pub struct MockResourceMeteringPubSubServer;

impl MockResourceMeteringPubSubServer {
    pub fn start(port: u16, credentials: Option<grpcio::ServerCredentials>) -> grpcio::Server {
        let env = Arc::new(grpcio::Environment::new(2));
        let channel_args = grpcio::ChannelBuilder::new(Arc::clone(&env))
            .max_concurrent_stream(2)
            .max_receive_message_len(-1)
            .max_send_message_len(-1)
            .build_args();

        let mut server_builder = grpcio::ServerBuilder::new(env).channel_args(channel_args);

        let host = "0.0.0.0";
        server_builder = match credentials {
            Some(credentials) => server_builder.bind_with_cred(host, port, credentials),
            None => server_builder.bind("0.0.0.0", port),
        };

        let mut server = server_builder
            .register_service(
                kvproto::resource_usage_agent_grpc::create_resource_metering_pub_sub(Self),
            )
            .build()
            .expect("failed to build mock resource metering pubsub server");

        server.start();
        server
    }
}

impl kvproto::resource_usage_agent_grpc::ResourceMeteringPubSub
    for MockResourceMeteringPubSubServer
{
    fn subscribe(
        &mut self,
        ctx: ::grpcio::RpcContext,
        _: kvproto::resource_usage_agent::ResourceMeteringRequest,
        mut sink: grpcio::ServerStreamingSink<kvproto::resource_usage_agent::ResourceUsageRecord>,
    ) {
        let dump_record =
            kvproto::resource_usage_agent::ResourceUsageRecord_oneof_record_oneof::Record(
                kvproto::resource_usage_agent::GroupTagRecord {
                    resource_group_tag: {
                        let mut tag = tipb::ResourceGroupTag::new();
                        tag.set_sql_digest(b"sql_digest".to_vec());
                        tag.set_plan_digest(b"plan_digest".to_vec());
                        tag.set_label(tipb::ResourceGroupTagLabel::ResourceGroupTagLabelRow);

                        tag.write_to_bytes().unwrap()
                    },
                    items: vec![kvproto::resource_usage_agent::GroupTagRecordItem {
                        timestamp_sec: 1655363650,
                        cpu_time_ms: 10,
                        read_keys: 20,
                        write_keys: 30,
                        ..Default::default()
                    }]
                    .into(),
                    ..Default::default()
                },
            );

        ctx.spawn(async move {
            sink.send((
                kvproto::resource_usage_agent::ResourceUsageRecord {
                    record_oneof: Some(dump_record),
                    ..Default::default()
                },
                grpcio::WriteFlags::default(),
            ))
            .await
            .unwrap();
        })
    }
}
