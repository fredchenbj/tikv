// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use futures::future::{err, ok};
use futures::sync::oneshot::{Receiver, Sender};
use futures::{self, Future};
use hyper::service::service_fn;
use hyper::{self, header, Body, Method, Request, Response, Server, StatusCode};
#[cfg(target_os = "linux")]
use pprof;
#[cfg(target_os = "linux")]
use prost::Message;
#[cfg(target_os = "linux")]
use regex::Regex;
use tokio_threadpool::{Builder, ThreadPool};

use std::net::SocketAddr;
use std::str::FromStr;

use tikv_util::timer::GLOBAL_TIMER_HANDLE;

use super::Result;
use tikv_util::collections::HashMap;
use tikv_util::metrics::dump;

pub struct StatusServer {
    thread_pool: ThreadPool,
    tx: Sender<()>,
    rx: Option<Receiver<()>>,
    addr: Option<SocketAddr>,
}

impl StatusServer {
    pub fn new(status_thread_pool_size: usize) -> Self {
        let thread_pool = Builder::new()
            .pool_size(status_thread_pool_size)
            .name_prefix("status-server-")
            .after_start(|| {
                info!("Status server started");
            })
            .before_stop(|| {
                info!("stopping status server");
            })
            .build();
        let (tx, rx) = futures::sync::oneshot::channel::<()>();
        StatusServer {
            thread_pool,
            tx,
            rx: Some(rx),
            addr: None,
        }
    }

    fn err_response<T>(status_code: StatusCode, message: T) -> Response<Body>
    where
        T: Into<Body>,
    {
        Response::builder()
            .status(status_code)
            .body(message.into())
            .unwrap()
    }

    #[cfg(target_os = "linux")]
    fn extract_thread_name(thread_name: &str) -> String {
        lazy_static! {
            static ref THREAD_NAME_RE: Regex =
                Regex::new(r"^(?P<thread_name>[a-z-_ :]+?)(-?\d)*$").unwrap();
            static ref THREAD_NAME_REPLACE_SEPERATOR_RE: Regex = Regex::new(r"[_ ]").unwrap();
        }

        THREAD_NAME_RE
            .captures(thread_name)
            .and_then(|cap| {
                cap.name("thread_name").map(|thread_name| {
                    THREAD_NAME_REPLACE_SEPERATOR_RE
                        .replace_all(thread_name.as_str(), "-")
                        .into_owned()
                })
            })
            .unwrap_or_else(|| thread_name.to_owned())
    }

    #[cfg(target_os = "linux")]
    fn frames_post_processor() -> impl Fn(&mut pprof::Frames) {
        move |frames| {
            let name = Self::extract_thread_name(&frames.thread_name);
            frames.thread_name = name;
        }
    }

    #[cfg(target_os = "linux")]
    pub fn dump_rsprof(
        seconds: u64,
        frequency: i32,
    ) -> Box<dyn Future<Item = pprof::Report, Error = pprof::Error> + Send> {
        match pprof::ProfilerGuard::new(frequency) {
            Ok(guard) => {
                info!(
                    "start profiling {} seconds with frequency {} /s",
                    seconds, frequency
                );

                let timer = GLOBAL_TIMER_HANDLE.clone();
                Box::new(
                    timer
                        .delay(std::time::Instant::now() + std::time::Duration::from_secs(seconds))
                        .then(
                            move |_| -> Box<
                                dyn Future<Item = pprof::Report, Error = pprof::Error> + Send,
                            > {
                                let _ = guard;
                                Box::new(
                                    match guard
                                        .report()
                                        .frames_post_processor(Self::frames_post_processor())
                                        .build()
                                    {
                                        Ok(report) => ok(report),
                                        Err(e) => err(e),
                                    },
                                )
                            },
                        ),
                )
            }
            Err(e) => Box::new(err(e)),
        }
    }

    #[cfg(target_os = "linux")]
    pub fn dump_rsperf_to_resp(
        req: Request<Body>,
    ) -> Box<dyn Future<Item = Response<Body>, Error = hyper::Error> + Send> {
        let query = match req.uri().query() {
            Some(query) => query,
            None => {
                return Box::new(ok(StatusServer::err_response(StatusCode::BAD_REQUEST, "")));
            }
        };
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        let seconds: u64 = match query_pairs.get("seconds") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(err) => {
                    return Box::new(ok(StatusServer::err_response(
                        StatusCode::BAD_REQUEST,
                        err.to_string(),
                    )));
                }
            },
            None => 10,
        };

        let frequency: i32 = match query_pairs.get("frequency") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(err) => {
                    return Box::new(ok(StatusServer::err_response(
                        StatusCode::BAD_REQUEST,
                        err.to_string(),
                    )));
                }
            },
            None => 99, // Default frequency of sampling. 99Hz to avoid coincide with special periods
        };
        drop(query_pairs);

        let prototype_content_type: hyper::http::HeaderValue =
            hyper::http::HeaderValue::from_str("application/protobuf").unwrap();
        Box::new(
            Self::dump_rsprof(seconds, frequency)
                .and_then(move |report| {
                    let mut body: Vec<u8> = Vec::new();
                    if req.headers().get("Content-Type") == Some(&prototype_content_type) {
                        match report.pprof() {
                            Ok(profile) => match profile.encode(&mut body) {
                                Ok(()) => {
                                    info!("write report successfully");
                                    Box::new(ok(StatusServer::err_response(StatusCode::OK, body)))
                                }
                                Err(err) => Box::new(ok(StatusServer::err_response(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    err.to_string(),
                                ))),
                            },
                            Err(err) => Box::new(ok(StatusServer::err_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                err.to_string(),
                            ))),
                        }
                    } else {
                        match report.flamegraph(&mut body) {
                            Ok(_) => {
                                info!("write report successfully");
                                Box::new(ok(StatusServer::err_response(StatusCode::OK, body)))
                            }
                            Err(err) => Box::new(ok(StatusServer::err_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                err.to_string(),
                            ))),
                        }
                    }
                })
                .or_else(|err| {
                    ok(StatusServer::err_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        err.to_string(),
                    ))
                }),
        )
    }

    pub fn start(&mut self, status_addr: String) -> Result<()> {
        let addr = SocketAddr::from_str(&status_addr)?;

        // TODO: support TLS for the status server.
        let builder = Server::try_bind(&addr)?;

        // Start to serve.
        let server = builder.serve(move || {
            // Create a status service.
            service_fn(
                move |req: Request<Body>| -> Box<
                    dyn Future<Item = Response<Body>, Error = hyper::Error> + Send,
                > {
                    let path = req.uri().path().to_owned();
                    let method = req.method().to_owned();

                    #[cfg(feature = "failpoints")]
                        {
                            if path.starts_with(FAIL_POINTS_REQUEST_PATH) {
                                return handle_fail_points_request(req);
                            }
                        }

                    match (method, path.as_ref()) {
                        (Method::GET, "/metrics") => Box::new(ok(Response::new(dump().into()))),
                        (Method::GET, "/status") => Box::new(ok(Response::default())),
                        (Method::GET, "/debug/pprof/profile") => {
                            #[cfg(target_os = "linux")]
                                { Self::dump_rsperf_to_resp(req) }
                            #[cfg(not(target_os = "linux"))]
                                { Box::new(ok(Response::default())) }
                        }
                        _ => Box::new(ok(StatusServer::err_response(
                            StatusCode::NOT_FOUND,
                            "path not found",
                        ))),
                    }
                },
            )
        });
        self.addr = Some(server.local_addr());
        let graceful = server
            .with_graceful_shutdown(self.rx.take().unwrap())
            .map_err(|e| error!("Status server error: {:?}", e));
        self.thread_pool.spawn(graceful);
        Ok(())
    }

    pub fn stop(self) {
        let _ = self.tx.send(());
        self.thread_pool
            .shutdown_now()
            .wait()
            .unwrap_or_else(|e| error!("failed to stop the status server, error: {:?}", e));
    }

    // Return listening address, this may only be used for outer test
    // to get the real address because we may use "127.0.0.1:0"
    // in test to avoid port conflict.
    pub fn listening_addr(&self) -> SocketAddr {
        self.addr.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::server::status_server::StatusServer;
    use futures::future::{lazy, Future};
    use hyper::{Client, StatusCode, Uri};

    #[test]
    fn test_status_service() {
        let mut status_server = StatusServer::new(1);
        let _ = status_server.start("127.0.0.1:0".to_string());
        let client = Client::new();
        let uri = Uri::builder()
            .scheme("http")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/metrics")
            .build()
            .unwrap();

        let handle = status_server.thread_pool.spawn_handle(lazy(move || {
            client
                .get(uri)
                .map(|res| {
                    assert_eq!(res.status(), StatusCode::OK);
                })
                .map_err(|err| {
                    panic!("response status is not OK: {:?}", err);
                })
        }));
        handle.wait().unwrap();
        status_server.stop();
    }

    #[test]
    fn test_config_endpoint() {
        let config = TiKvConfig::default();
        let mut status_server = StatusServer::new(1, config);
        let _ = status_server.start("127.0.0.1:0".to_string());
        let client = Client::new();
        let uri = Uri::builder()
            .scheme("http")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/config")
            .build()
            .unwrap();
        let handle = status_server.thread_pool.spawn_handle(lazy(move || {
            client
                .get(uri)
                .and_then(|resp| {
                    assert_eq!(resp.status(), StatusCode::OK);
                    resp.into_body().concat2()
                })
                .map(|body| {
                    let v = body.to_vec();
                    let resp_json = String::from_utf8_lossy(&v).to_string();
                    let cfg = TiKvConfig::default();
                    serde_json::to_string(&cfg)
                        .map(|cfg_json| {
                            assert_eq!(resp_json, cfg_json);
                        })
                        .expect("Could not convert TiKvConfig to string");
                })
                .map_err(|err| panic!("response status is not OK: {:?}", err))
        }));
        handle.wait().unwrap();
        status_server.stop();
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn test_status_service_fail_endpoints() {
        let _guard = fail::FailScenario::setup();
        let config = TiKvConfig::default();
        let mut status_server = StatusServer::new(1, config);
        let _ = status_server.start("127.0.0.1:0".to_string());
        let client = Client::new();
        let addr = status_server.listening_addr().to_string();

        let handle = status_server.thread_pool.spawn_handle(lazy(move || {
            // test add fail point
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail/test_fail_point_name")
                .build()
                .unwrap();
            let mut req = Request::new(Body::from("panic"));
            *req.method_mut() = Method::PUT;
            *req.uri_mut() = uri.clone();

            let future_1_add_fail_point = client
                .request(req)
                .map(|res| {
                    assert_eq!(res.status(), StatusCode::OK);

                    let list: Vec<String> = fail::list()
                        .into_iter()
                        .map(move |(name, actions)| format!("{}={}", name, actions))
                        .collect();
                    assert_eq!(list.len(), 1);
                    let list = list.join(";");
                    assert_eq!("test_fail_point_name=panic", list);
                })
                .map_err(|err| {
                    panic!("response status is not OK: {:?}", err);
                });

            // test add another fail point
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail/and_another_name")
                .build()
                .unwrap();
            let mut req = Request::new(Body::from("panic"));
            *req.method_mut() = Method::PUT;
            *req.uri_mut() = uri.clone();

            let future_2_add_fail_point = client
                .request(req)
                .map(|res| {
                    assert_eq!(res.status(), StatusCode::OK);

                    let list: Vec<String> = fail::list()
                        .into_iter()
                        .map(move |(name, actions)| format!("{}={}", name, actions))
                        .collect();
                    assert_eq!(2, list.len());
                    let list = list.join(";");
                    assert!(list.contains("test_fail_point_name=panic"));
                    assert!(list.contains("and_another_name=panic"))
                })
                .map_err(|err| {
                    panic!("response status is not OK: {:?}", err);
                });

            // test list fail points
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail")
                .build()
                .unwrap();
            let mut req = Request::default();
            *req.method_mut() = Method::GET;
            *req.uri_mut() = uri.clone();

            let future_3_list_fail_points = client
                .request(req)
                .and_then(|res| {
                    assert_eq!(res.status(), StatusCode::OK);
                    res.into_body().concat2()
                })
                .map(|chunk| {
                    let body = chunk.iter().cloned().collect::<Vec<u8>>();
                    let body = String::from_utf8(body).unwrap();
                    assert!(body.contains("test_fail_point_name=panic"));
                    assert!(body.contains("and_another_name=panic"))
                })
                .map_err(|err| {
                    panic!("response status is not OK: {:?}", err);
                });

            // test delete fail point
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail/test_fail_point_name")
                .build()
                .unwrap();
            let mut req = Request::default();
            *req.method_mut() = Method::DELETE;
            *req.uri_mut() = uri.clone();

            let future_4_delete_fail_points = client
                .request(req)
                .map(|res| {
                    assert_eq!(res.status(), StatusCode::OK);

                    let list: Vec<String> = fail::list()
                        .into_iter()
                        .map(move |(name, actions)| format!("{}={}", name, actions))
                        .collect();
                    assert_eq!(1, list.len());
                    let list = list.join(";");
                    assert_eq!("and_another_name=panic", list);
                })
                .map_err(|err| {
                    panic!("response status is not OK: {:?}", err);
                });

            future_1_add_fail_point
                .then(|_| future_2_add_fail_point)
                .then(|_| future_3_list_fail_points)
                .then(|_| future_4_delete_fail_points)
        }));

        handle.wait().unwrap();
        status_server.stop();
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn test_status_service_fail_endpoints_can_trigger_fails() {
        let _guard = fail::FailScenario::setup();
        let config = TiKvConfig::default();
        let mut status_server = StatusServer::new(1, config);
        let _ = status_server.start("127.0.0.1:0".to_string());
        let client = Client::new();
        let addr = status_server.listening_addr().to_string();

        let handle = status_server.thread_pool.spawn_handle(lazy(move || {
            // test add fail point
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail/a_test_fail_name_nobody_else_is_using")
                .build()
                .unwrap();
            let mut req = Request::new(Body::from("return"));
            *req.method_mut() = Method::PUT;
            *req.uri_mut() = uri.clone();

            client
                .request(req)
                .map(|res| {
                    assert_eq!(res.status(), StatusCode::OK);
                })
                .map_err(|err| {
                    panic!("response status is not OK: {:?}", err);
                })
        }));

        handle.wait().unwrap();
        status_server.stop();

        let true_only_if_fail_point_triggered = || {
            fail_point!("a_test_fail_name_nobody_else_is_using", |_| { true });
            false
        };
        assert!(true_only_if_fail_point_triggered());
    }

    #[cfg(not(feature = "failpoints"))]
    #[test]
    fn test_status_service_fail_endpoints_should_give_404_when_failpoints_are_disable() {
        let _guard = fail::FailScenario::setup();
        let config = TiKvConfig::default();
        let mut status_server = StatusServer::new(1, config);
        let _ = status_server.start("127.0.0.1:0".to_string());
        let client = Client::new();
        let addr = status_server.listening_addr().to_string();

        let handle = status_server.thread_pool.spawn_handle(lazy(move || {
            // test add fail point
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail/a_test_fail_name_nobody_else_is_using")
                .build()
                .unwrap();
            let mut req = Request::new(Body::from("panic"));
            *req.method_mut() = Method::PUT;
            *req.uri_mut() = uri.clone();

            client
                .request(req)
                .map(|res| {
                    // without feature "failpoints", this PUT endpoint should return 404
                    assert_eq!(res.status(), StatusCode::NOT_FOUND);
                })
                .map_err(|err| {
                    panic!("response status is not OK: {:?}", err);
                })
        }));

        handle.wait().unwrap();
        status_server.stop();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_extract_thread_name() {
        assert_eq!(
            &StatusServer::extract_thread_name("test-name-1"),
            "test-name"
        );
        assert_eq!(
            &StatusServer::extract_thread_name("grpc-server-5"),
            "grpc-server"
        );
        assert_eq!(
            &StatusServer::extract_thread_name("rocksdb:bg1000"),
            "rocksdb:bg"
        );
        assert_eq!(
            &StatusServer::extract_thread_name("raftstore-1-100"),
            "raftstore"
        );
        assert_eq!(
            &StatusServer::extract_thread_name("snap sender1000"),
            "snap-sender"
        );
        assert_eq!(
            &StatusServer::extract_thread_name("snap_sender1000"),
            "snap-sender"
        );
    }
}
