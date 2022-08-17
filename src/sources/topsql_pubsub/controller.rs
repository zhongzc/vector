use std::collections::{HashMap, HashSet};
use std::time::Duration;

use snafu::{ResultExt, Snafu};
use vector_common::shutdown::ShutdownSignal;

use crate::sources::topsql_pubsub::shutdown::{ShutdownNotifier, ShutdownSubscriber};
use crate::sources::topsql_pubsub::topology::{Component, FetchError, TopologyFetcher};
use crate::SourceSender;

#[derive(Debug, Snafu)]
pub enum ControllError {
    #[snafu(display("Failed to get topology: {}", source))]
    GetTopology { source: FetchError },
}

pub struct Controller {
    topo_fetch_interval: Duration,
    topo_fetcher: TopologyFetcher,

    components: HashSet<Component>,
    running_components: HashMap<Component, ShutdownNotifier>,

    shutdown_notifier: ShutdownNotifier,
    shutdown_subscriber: ShutdownSubscriber,

    // from source context
    out: SourceSender,
}

impl Controller {
    async fn run(mut self, mut shutdown: ShutdownSignal) {
        loop {
            tokio::select! {
                res = self.fetch_and_update() => match res {
                    Ok(has_change) if has_change => {
                        info!(message = "Topology has changed", latest_components = ?self.components);
                    }
                    Err(error) => {
                        error!(message = "Failed to fetch topology", error = %error);
                    }
                    _ => {}
                },
                _ = &mut shutdown => {
                    info!("TopSQL PubSub Controller is shutting down");
                    break;
                },
            }

            tokio::select! {
                _ = tokio::time::sleep(self.topo_fetch_interval) => {}
                _ = &mut shutdown => {
                    info!("TopSQL PubSub Controller is shutting down");
                    break;
                }
            }
        }

        self.shutdown_all_components().await;
    }

    async fn fetch_and_update(&mut self) -> Result<bool, ControllError> {
        let mut has_change = false;
        let mut latest_components = HashSet::new();
        self.topo_fetcher
            .get_up_components(&mut latest_components)
            .await
            .context(GetTopologySnafu)?;

        let newcomers = latest_components.difference(&self.components);
        let leavers = self.components.difference(&latest_components);

        for newcomer in newcomers {
            has_change = true;
            println!("New component: {:?}", newcomer);
        }
        for leaver in leavers {
            has_change = true;
            println!("Leaving component: {:?}", leaver);
        }

        self.components = latest_components;
        Ok(has_change)
    }

    async fn shutdown_all_components(mut self) {
        for (component, shutdown_notifier) in self.running_components {
            shutdown_notifier.shutdown();
            info!(message = "Shutting down TopSQL component", component = %component);
            shutdown_notifier.wait_for_exit().await;
        }

        drop(self.shutdown_subscriber);
        self.shutdown_notifier.shutdown();
        self.shutdown_notifier.wait_for_exit().await;
        info!(message = "All TopSQL components have been shut down");
    }
}
