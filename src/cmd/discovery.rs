use anyhow::Result;
use clap::Args as ClapArgs;
use itertools::Itertools;
use k8s_openapi::{
    api::{
        core::v1::{Pod, PodStatus, Service, ServiceSpec},
        discovery::v1::EndpointSlice,
    },
    apimachinery::pkg::apis::meta::v1::LabelSelector,
};
use kube::{api::ListParams, core::Selector, Api, ResourceExt};

use super::GlobalArgs;

/// Discover the seed node to bootstrap the cluster. This has been implemented
/// for Kubernetes.
///
/// If there are healthy pods, it will return the service name. If there are no
/// healthy pods, it will return the hostname of the *newest* pod which can be
/// itself. This helps maintain state in the following scenarios:
///
/// 1. A deployment updates a single pod at a time. The new pod will use the
///    normal service to bootstrap.
/// 1. A pod is restarted. The pod will use the normal service to bootstrap.
/// 1. A new deployment is created. The replicas will use the *first* pod to be
///    created to bootstrap.
///
/// Note: if you are not using persistent volumes, you may loose data if all the
/// pods restart at the same time.
#[derive(Debug, ClapArgs)]
pub struct Args {
    /// Service to use for discovery. This will be used if there are some ready
    /// endpoints.
    service: String,
}

impl Args {
    pub async fn run(self, _: GlobalArgs) {
        match run(self.service.as_str()).await {
            Ok(service) => println!("{}", service),
            Err(e) => {
                eprintln!("Error running discovery: {}", e);
                std::process::exit(1);
            }
        }
    }
}

async fn by_endpoints(client: kube::Client, service: &str) -> Result<Option<String>> {
    let slices = Api::<EndpointSlice>::default_namespaced(client.clone())
        .list(
            &ListParams::default()
                .labels(format!("kubernetes.io/service-name={}", service).as_str()),
        )
        .await?;

    let endpoints = slices
        .into_iter()
        .flat_map(|slice| slice.endpoints)
        .filter(|endpoint| {
            endpoint
                .conditions
                .as_ref()
                .map(|c| c.ready.unwrap_or_default())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();

    if endpoints.is_empty() {
        return Ok(None);
    }

    Ok(Some(service.to_string()))
}

async fn by_pods(client: kube::Client, service: &str) -> Result<String> {
    let service_resource = Api::<Service>::default_namespaced(client.clone())
        .get(service)
        .await?;

    let Some(ServiceSpec { selector, .. }) = &service_resource.spec else {
        return Err(anyhow::anyhow!("service {} has no selector", service));
    };

    let label_selector: Selector = LabelSelector {
        match_labels: selector.clone(),
        ..Default::default()
    }
    .try_into()?;

    let Some(pod) = Api::<Pod>::default_namespaced(client.clone())
        .list(&ListParams::default().labels_from(&label_selector))
        .await?
        .items
        .into_iter()
        .sorted_by_key(ResourceExt::creation_timestamp)
        .next()
    else {
        return Err(anyhow::anyhow!("no pods found for {}", service,));
    };

    if let Some(PodStatus {
        pod_ip: Some(pod_ip),
        ..
    }) = pod.status
    {
        return Ok(pod_ip);
    }

    Err(anyhow::anyhow!("pod {} has no IP", pod.name_any()))
}

async fn run(service: &str) -> Result<String> {
    let client = kube::Client::try_default().await?;

    if let Some(service) = by_endpoints(client.clone(), service).await? {
        return Ok(service);
    }

    by_pods(client.clone(), service).await
}
