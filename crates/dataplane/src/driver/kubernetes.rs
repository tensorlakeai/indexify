//! Kubernetes Pod driver for the Indexify dataplane.
//!
//! Implements [`ProcessDriver`] by creating and managing Kubernetes Pods.
//! Function executor pods run the `function-executor` script, which is
//! provided by the tensorlake Python SDK and present in every function image.
//! Sandbox pods inject the `indexify-container-daemon` binary via an
//! initContainer.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use k8s_openapi::{
    api::core::v1::{
        Container,
        EmptyDirVolumeSource,
        EnvVar,
        LocalObjectReference,
        Pod,
        PodSpec,
        ResourceRequirements,
        Volume,
        VolumeMount,
    },
    apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::ObjectMeta},
};
use kube::{
    Api,
    Client,
    api::{DeleteParams, ListParams, LogParams, PostParams},
};
use tokio::time::{Instant, sleep};
use tracing::info;

use super::{
    DAEMON_GRPC_PORT,
    DAEMON_HTTP_PORT,
    ExitStatus,
    ProcessConfig,
    ProcessDriver,
    ProcessHandle,
    ProcessType,
};

const FUNCTION_EXECUTOR_PORT: u16 = 9600;
const POD_READY_TIMEOUT_SECS: u64 = 120;
const POD_POLL_INTERVAL_MS: u64 = 500;

/// Label applied to every pod created by the dataplane — used for listing
/// managed pods during reconciliation.
pub const MANAGED_LABEL: &str = "tensorlake.ai/managed";
const TYPE_LABEL: &str = "tensorlake.ai/type";
const CONTAINER_ID_LABEL: &str = "tensorlake.ai/container-id";

pub struct KubernetesPodDriver {
    client: Client,
    /// Namespace where function/sandbox pods are created.
    namespace: String,
    /// The dataplane's own container image. Used as the initContainer source
    /// for injecting the `indexify-container-daemon` binary into sandbox pods.
    dataplane_image: String,
    /// Optional ServiceAccount name to assign to created pods.
    pod_service_account: Option<String>,
    /// Optional node selector labels applied to created pods.
    pod_node_selector: Option<BTreeMap<String, String>>,
    /// imagePullSecret names attached to created pods for private registry access.
    pod_image_pull_secrets: Vec<String>,
}

impl KubernetesPodDriver {
    /// Create a new driver using the in-cluster or kubeconfig credentials.
    pub async fn new(
        namespace: String,
        dataplane_image: String,
        pod_service_account: Option<String>,
        pod_node_selector: Option<BTreeMap<String, String>>,
        pod_image_pull_secrets: Vec<String>,
    ) -> Result<Self> {
        let client = Client::try_default()
            .await
            .context("Failed to create Kubernetes client")?;
        Ok(Self {
            client,
            namespace,
            dataplane_image,
            pod_service_account,
            pod_node_selector,
            pod_image_pull_secrets,
        })
    }

    fn pods(&self) -> Api<Pod> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    async fn delete_pod(&self, pod_name: &str, grace_period_seconds: Option<u32>) -> Result<()> {
        match self
            .pods()
            .delete(
                pod_name,
                &DeleteParams {
                    grace_period_seconds,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
            Err(e) => Err(e).context(format!("Failed to delete pod {pod_name}")),
        }
    }

    /// Poll until the pod has been assigned a pod IP, or the deadline passes.
    async fn wait_for_pod_ip(&self, pod_name: &str) -> Result<String> {
        let api = self.pods();
        let deadline = Instant::now() + Duration::from_secs(POD_READY_TIMEOUT_SECS);
        loop {
            let pod = api
                .get(pod_name)
                .await
                .with_context(|| format!("Failed to get pod {pod_name}"))?;

            let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref());
            if matches!(phase, Some("Failed") | Some("Unknown")) {
                return Err(anyhow::anyhow!(
                    "Pod {pod_name} entered terminal state: {:?}",
                    phase
                ));
            }

            if let Some(ip) = pod
                .status
                .as_ref()
                .and_then(|s| s.pod_ip.as_deref())
                .filter(|ip| !ip.is_empty())
            {
                return Ok(ip.to_string());
            }

            if Instant::now() >= deadline {
                return Err(anyhow::anyhow!(
                    "Timed out waiting for pod {pod_name} to get an IP after {}s",
                    POD_READY_TIMEOUT_SECS
                ));
            }
            sleep(Duration::from_millis(POD_POLL_INTERVAL_MS)).await;
        }
    }

    fn base_labels(&self, container_id: &str, pod_type: &str) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert(MANAGED_LABEL.to_string(), "true".to_string());
        labels.insert(TYPE_LABEL.to_string(), pod_type.to_string());
        labels.insert(CONTAINER_ID_LABEL.to_string(), container_id.to_string());
        labels
    }

    fn resource_requirements(config: &ProcessConfig) -> Option<ResourceRequirements> {
        config.resources.as_ref().map(|r| {
            let mut requests = BTreeMap::new();
            let mut limits = BTreeMap::new();
            if let Some(cpu) = r.cpu_millicores {
                let v = Quantity(format!("{}m", cpu));
                requests.insert("cpu".to_string(), v.clone());
                limits.insert("cpu".to_string(), v);
            }
            if let Some(mem) = r.memory_bytes {
                let v = Quantity(mem.to_string());
                requests.insert("memory".to_string(), v.clone());
                limits.insert("memory".to_string(), v);
            }
            ResourceRequirements {
                requests: Some(requests),
                limits: Some(limits),
                ..Default::default()
            }
        })
    }

    fn env_vars(config: &ProcessConfig) -> Vec<EnvVar> {
        config
            .env
            .iter()
            .map(|(k, v)| EnvVar {
                name: k.clone(),
                value: Some(v.clone()),
                ..Default::default()
            })
            .collect()
    }

    fn apply_pod_spec_overrides(&self, spec: &mut PodSpec) {
        if let Some(sa) = &self.pod_service_account {
            if !sa.is_empty() {
                spec.service_account_name = Some(sa.clone());
            }
        }
        if let Some(ns) = &self.pod_node_selector {
            spec.node_selector = Some(ns.clone());
        }
        if !self.pod_image_pull_secrets.is_empty() {
            spec.image_pull_secrets = Some(
                self.pod_image_pull_secrets
                    .iter()
                    .map(|name| LocalObjectReference { name: Some(name.clone()) })
                    .collect(),
            );
        }
    }

    fn build_function_pod(&self, config: &ProcessConfig, image: &str) -> Pod {
        let env_vars = Self::env_vars(config);
        let resources = Self::resource_requirements(config);
        let labels = self.base_labels(&config.id, "function");

        let mut args = config.args.clone();
        args.push("--address".to_string());
        args.push(format!("0.0.0.0:{}", FUNCTION_EXECUTOR_PORT));

        // The `function-executor` script is provided by the tensorlake Python SDK,
        // which is injected into every function image at build time. No initContainer
        // needed.
        let command = if config.command.is_empty() {
            None
        } else {
            Some(vec![config.command.clone()])
        };

        let main_container = Container {
            name: "function-executor".to_string(),
            image: Some(image.to_string()),
            command,
            args: Some(args),
            env: Some(env_vars),
            resources,
            ..Default::default()
        };

        let mut pod_spec = PodSpec {
            containers: vec![main_container],
            restart_policy: Some("Never".to_string()),
            ..Default::default()
        };
        self.apply_pod_spec_overrides(&mut pod_spec);

        Pod {
            metadata: ObjectMeta {
                name: Some(format!("indexify-fn-{}", config.id)),
                namespace: Some(self.namespace.clone()),
                labels: Some(labels),
                ..Default::default()
            },
            spec: Some(pod_spec),
            ..Default::default()
        }
    }

    fn build_sandbox_pod(&self, config: &ProcessConfig, image: &str) -> Pod {
        let env_vars = Self::env_vars(config);
        let resources = Self::resource_requirements(config);
        let labels = self.base_labels(&config.id, "sandbox");

        let daemon_mount_path = "/indexify-daemon-bin";
        let daemon_bin = format!("{}/indexify-daemon", daemon_mount_path);

        // initContainer copies the daemon binary from the dataplane image.
        let init = Container {
            name: "copy-daemon".to_string(),
            image: Some(self.dataplane_image.clone()),
            command: Some(vec!["sh".to_string(), "-c".to_string()]),
            args: Some(vec![format!(
                "cp /indexify/indexify-container-daemon {path}/indexify-daemon \
                 && chmod +x {path}/indexify-daemon",
                path = daemon_mount_path
            )]),
            volume_mounts: Some(vec![VolumeMount {
                name: "daemon-bin".to_string(),
                mount_path: daemon_mount_path.to_string(),
                ..Default::default()
            }]),
            ..Default::default()
        };

        let mut daemon_args = vec![
            "--port".to_string(),
            DAEMON_GRPC_PORT.to_string(),
            "--http-port".to_string(),
            DAEMON_HTTP_PORT.to_string(),
            "--log-dir".to_string(),
            "/var/log/indexify".to_string(),
        ];
        if !config.command.is_empty() {
            daemon_args.push("--".to_string());
            daemon_args.push(config.command.clone());
            daemon_args.extend(config.args.clone());
        }

        let main_container = Container {
            name: "sandbox".to_string(),
            image: Some(image.to_string()),
            command: Some(vec![daemon_bin]),
            args: Some(daemon_args),
            env: Some(env_vars),
            resources,
            volume_mounts: Some(vec![VolumeMount {
                name: "daemon-bin".to_string(),
                mount_path: daemon_mount_path.to_string(),
                ..Default::default()
            }]),
            ..Default::default()
        };

        let mut pod_spec = PodSpec {
            init_containers: Some(vec![init]),
            containers: vec![main_container],
            restart_policy: Some("Never".to_string()),
            volumes: Some(vec![Volume {
                name: "daemon-bin".to_string(),
                empty_dir: Some(EmptyDirVolumeSource::default()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        self.apply_pod_spec_overrides(&mut pod_spec);

        Pod {
            metadata: ObjectMeta {
                name: Some(format!("indexify-sb-{}", config.id)),
                namespace: Some(self.namespace.clone()),
                labels: Some(labels),
                ..Default::default()
            },
            spec: Some(pod_spec),
            ..Default::default()
        }
    }
}

#[async_trait]
impl ProcessDriver for KubernetesPodDriver {
    async fn start(&self, config: ProcessConfig) -> Result<ProcessHandle> {
        let image = config
            .image
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Kubernetes driver requires an image"))?;

        let (pod, grpc_port, http_port) = match config.process_type {
            ProcessType::Function => {
                let pod = self.build_function_pod(&config, image);
                (pod, FUNCTION_EXECUTOR_PORT, None::<u16>)
            }
            ProcessType::Sandbox => {
                let pod = self.build_sandbox_pod(&config, image);
                (pod, DAEMON_GRPC_PORT, Some(DAEMON_HTTP_PORT))
            }
        };

        let pod_name = pod.metadata.name.clone().unwrap();
        info!(pod = %pod_name, image = %image, "Creating Kubernetes pod");

        let create_result = self.pods().create(&PostParams::default(), &pod).await;
        match create_result {
            Ok(_) => {}
            // Pod already exists from a previous dataplane run — delete it and
            // wait for it to be gone, then recreate with the (potentially updated)
            // image from the ConfigMap.
            Err(kube::Error::Api(ref e)) if e.code == 409 => {
                info!(pod = %pod_name, "Pod already exists, deleting before recreation");
                let _ = self
                    .pods()
                    .delete(
                        &pod_name,
                        &DeleteParams {
                            grace_period_seconds: Some(0),
                            ..Default::default()
                        },
                    )
                    .await;
                // Wait for the pod to disappear before recreating.
                let deadline = Instant::now() + Duration::from_secs(30);
                loop {
                    match self.pods().get(&pod_name).await {
                        Err(kube::Error::Api(e)) if e.code == 404 => break,
                        _ if Instant::now() >= deadline => {
                            return Err(anyhow::anyhow!(
                                "Timed out waiting for old pod {pod_name} to be deleted"
                            ));
                        }
                        _ => sleep(Duration::from_millis(500)).await,
                    }
                }
                self.pods()
                    .create(&PostParams::default(), &pod)
                    .await
                    .with_context(|| {
                        format!("Failed to create pod {pod_name} after conflict resolution")
                    })?;
            }
            Err(e) => {
                return Err(e).with_context(|| format!("Failed to create pod {pod_name}"));
            }
        }

        let pod_ip = match self.wait_for_pod_ip(&pod_name).await {
            Ok(ip) => ip,
            Err(e) => {
                // Best-effort cleanup: delete the pod we just created.
                let _ = self.delete_pod(&pod_name, Some(0)).await;
                return Err(e);
            }
        };

        info!(pod = %pod_name, ip = %pod_ip, "Kubernetes pod scheduled");

        Ok(ProcessHandle {
            id: pod_name,
            daemon_addr: Some(format!("{pod_ip}:{grpc_port}")),
            http_addr: http_port.map(|p| format!("{pod_ip}:{p}")),
            container_ip: pod_ip,
        })
    }

    async fn send_sig(&self, handle: &ProcessHandle, signal: i32) -> Result<()> {
        let grace_period_seconds = match signal {
            9 => Some(0), // SIGKILL — delete immediately
            15 => None,   // SIGTERM — use the pod's terminationGracePeriodSeconds
            _ => return Err(anyhow::anyhow!("Unsupported signal: {}", signal)),
        };
        self.delete_pod(&handle.id, grace_period_seconds).await
    }

    async fn stop(&self, handle: &ProcessHandle, _timeout_secs: u64) -> Result<()> {
        // SIGTERM semantics: respect the pod's terminationGracePeriodSeconds.
        self.delete_pod(&handle.id, None).await
    }

    async fn kill(&self, handle: &ProcessHandle) -> Result<()> {
        self.delete_pod(&handle.id, Some(0)).await
    }

    async fn alive(&self, handle: &ProcessHandle) -> Result<bool> {
        match self.pods().get(&handle.id).await {
            Ok(pod) => {
                let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref());
                Ok(matches!(phase, Some("Running") | Some("Pending")))
            }
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(false),
            Err(e) => Err(e).context("Failed to inspect pod"),
        }
    }

    async fn get_exit_status(&self, handle: &ProcessHandle) -> Result<Option<ExitStatus>> {
        match self.pods().get(&handle.id).await {
            Ok(pod) => {
                let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref());
                match phase {
                    Some("Succeeded") => Ok(Some(ExitStatus {
                        exit_code: Some(0),
                        oom_killed: false,
                    })),
                    Some("Failed") => {
                        let terminated = pod
                            .status
                            .as_ref()
                            .and_then(|s| s.container_statuses.as_ref())
                            .and_then(|cs| cs.first())
                            .and_then(|cs| cs.state.as_ref())
                            .and_then(|state| state.terminated.as_ref());
                        Ok(Some(ExitStatus {
                            exit_code: terminated.map(|t| t.exit_code as i64),
                            oom_killed: terminated
                                .and_then(|t| t.reason.as_deref())
                                .map(|r| r == "OOMKilled")
                                .unwrap_or(false),
                        }))
                    }
                    _ => Ok(None),
                }
            }
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(None),
            Err(e) => Err(e).context("Failed to get pod exit status"),
        }
    }

    async fn list_containers(&self) -> Result<Vec<String>> {
        let lp = ListParams::default().labels(&format!("{MANAGED_LABEL}=true"));
        match self.pods().list(&lp).await {
            Ok(pods) => Ok(pods
                .items
                .into_iter()
                .filter_map(|p| p.metadata.name)
                .collect()),
            Err(e) => Err(e).context("Failed to list managed pods"),
        }
    }

    async fn get_logs(&self, handle: &ProcessHandle, tail: u32) -> Result<String> {
        let params = LogParams {
            tail_lines: Some(tail as i64),
            ..Default::default()
        };
        self.pods()
            .logs(&handle.id, &params)
            .await
            .context("Failed to get pod logs")
    }
}
