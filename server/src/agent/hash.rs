use sha2::{Digest, Sha256};

use crate::executor_api::executor_api_pb;

pub struct HostResourcesHasher;

impl HostResourcesHasher {
    pub fn update(hasher: &mut Sha256, r: &executor_api_pb::HostResources) {
        if let Some(c) = r.cpu_count {
            hasher.update(c.to_le_bytes());
        }
        if let Some(m) = r.memory_bytes {
            hasher.update(m.to_le_bytes());
        }
        if let Some(d) = r.disk_bytes {
            hasher.update(d.to_le_bytes());
        }
        if let Some(ref g) = r.gpu {
            if let Some(cnt) = g.count {
                hasher.update(cnt.to_le_bytes());
            }
            if let Some(model) = g.model {
                hasher.update(model.to_le_bytes());
            }
        }
    }
}

pub struct FunctionExecutorStatesHasher;

impl FunctionExecutorStatesHasher {
    pub fn update(hasher: &mut Sha256, states: &[executor_api_pb::FunctionExecutorState]) {
        // Sort by FE id for determinism
        let mut festates = states.to_vec();
        festates.sort_by(|a, b| {
            let a_id = a
                .description
                .as_ref()
                .and_then(|d| d.id.clone())
                .unwrap_or_default();
            let b_id = b
                .description
                .as_ref()
                .and_then(|d| d.id.clone())
                .unwrap_or_default();
            a_id.cmp(&b_id)
        });
        for st in festates {
            if let Some(ref desc) = st.description {
                if let Some(ref id) = desc.id {
                    hasher.update(id.as_bytes());
                }
                if let Some(ref f) = desc.function {
                    if let Some(ref ns) = f.namespace {
                        hasher.update(ns.as_bytes());
                    }
                    if let Some(ref app) = f.application_name {
                        hasher.update(app.as_bytes());
                    }
                    if let Some(ref name) = f.function_name {
                        hasher.update(name.as_bytes());
                    }
                    if let Some(ref ver) = f.application_version {
                        hasher.update(ver.as_bytes());
                    }
                }
                if let Some(ref res) = desc.resources {
                    if let Some(cpu) = res.cpu_ms_per_sec {
                        hasher.update(cpu.to_le_bytes());
                    }
                    if let Some(mem) = res.memory_bytes {
                        hasher.update(mem.to_le_bytes());
                    }
                    if let Some(disk) = res.disk_bytes {
                        hasher.update(disk.to_le_bytes());
                    }
                    if let Some(ref g) = res.gpu {
                        if let Some(cnt) = g.count {
                            hasher.update(cnt.to_le_bytes());
                        }
                        if let Some(model) = g.model {
                            hasher.update(model.to_le_bytes());
                        }
                    }
                }
                if let Some(mc) = desc.max_concurrency {
                    hasher.update(mc.to_le_bytes());
                }
                if let Some(init) = desc.initialization_timeout_ms {
                    hasher.update(init.to_le_bytes());
                }
                if let Some(alloc) = desc.allocation_timeout_ms {
                    hasher.update(alloc.to_le_bytes());
                }
            }
            if let Some(status) = st.status {
                hasher.update(status.to_le_bytes());
            }
            if let Some(reason) = st.termination_reason {
                hasher.update(reason.to_le_bytes());
            }
            for a in st.allocation_ids_caused_termination {
                hasher.update(a.as_bytes());
            }
        }
    }
}
