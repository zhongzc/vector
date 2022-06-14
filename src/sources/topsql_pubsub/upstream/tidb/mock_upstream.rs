use std::sync::Arc;

use futures::SinkExt;

#[derive(Clone)]
pub struct MockTopSqlPubSubServer;

impl MockTopSqlPubSubServer {
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
            .register_service(tipb::create_top_sql_pub_sub(Self))
            .build()
            .expect("failed to build mock topsql pubsub server");

        server.start();
        server
    }
}

impl tipb::TopSqlPubSub for MockTopSqlPubSubServer {
    fn subscribe(
        &mut self,
        ctx: grpcio::RpcContext,
        _: tipb::TopSqlSubRequest,
        mut sink: grpcio::ServerStreamingSink<tipb::TopSqlSubResponse>,
    ) {
        let dump_record = tipb::TopSqlRecord {
            sql_digest: b"sql_digest".to_vec(),
            plan_digest: b"plan_digest".to_vec(),
            items: vec![tipb::TopSqlRecordItem {
                timestamp_sec: 1655363650,
                cpu_time_ms: 10,
                stmt_exec_count: 20,
                stmt_kv_exec_count: (vec![("127.0.0.1:20180".to_owned(), 10)])
                    .into_iter()
                    .collect(),
                stmt_duration_sum_ns: 30,
                stmt_duration_count: 20,
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        };

        let dump_sql_meta = tipb::SqlMeta {
            sql_digest: b"sql_digest".to_vec(),
            normalized_sql: "sql_text".to_owned(),
            is_internal_sql: false,
            ..Default::default()
        };

        let dump_plan_meta = tipb::PlanMeta {
            plan_digest: b"plan_digest".to_vec(),
            normalized_plan: "plan_text".to_owned(),
            encoded_normalized_plan: "encoded_plan".to_owned(),
            ..Default::default()
        };

        ctx.spawn(async move {
            sink.send((
                tipb::TopSqlSubResponse {
                    resp_oneof: Some(tipb::TopSQLSubResponse_oneof_resp_oneof::Record(
                        dump_record,
                    )),
                    ..Default::default()
                },
                grpcio::WriteFlags::default(),
            ))
            .await
            .unwrap();
            sink.send((
                tipb::TopSqlSubResponse {
                    resp_oneof: Some(tipb::TopSQLSubResponse_oneof_resp_oneof::SqlMeta(
                        dump_sql_meta,
                    )),
                    ..Default::default()
                },
                grpcio::WriteFlags::default(),
            ))
            .await
            .unwrap();
            sink.send((
                tipb::TopSqlSubResponse {
                    resp_oneof: Some(tipb::TopSQLSubResponse_oneof_resp_oneof::PlanMeta(
                        dump_plan_meta,
                    )),
                    ..Default::default()
                },
                grpcio::WriteFlags::default(),
            ))
            .await
            .unwrap();
        });
    }
}
