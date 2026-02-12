#[cfg(test)]
mod tests {

    use std::collections::{HashMap, HashSet};

    use crate::data_model::{
        ContainerBuilder, ContainerPoolId, ContainerResources, ContainerState, ContainerType,
        ExecutorId, ExecutorServerMetadata, ExecutorServerMetadataBuilder, FunctionResources,
        GPU_MODEL_NVIDIA_A10, GPU_MODEL_NVIDIA_A100_40GB, GPU_MODEL_NVIDIA_H100_80GB, GPUResources,
        HostResources,
    };

    #[test]
    fn test_can_handle_function_resources() {
        struct Case {
            description: &'static str,
            host_resources: HostResources,
            func_resources: FunctionResources,
            expected_can_handle: bool,
        }
        let cases = vec![
            Case {
                description: "enough resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_can_handle: true,
            },
            Case {
                description: "enough resources with exact match of node and host resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu_configs: vec![],
                },
                expected_can_handle: true,
            },
            Case {
                description: "not enough cpu",
                host_resources: HostResources {
                    cpu_ms_per_sec: 500,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 501,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_can_handle: false,
            },
            Case {
                description: "not enough memory",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 512 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_can_handle: false,
            },
            Case {
                description: "not enough disk",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 512 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_can_handle: false,
            },
            Case {
                description: "with gpu - enough resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![
                        GPUResources {
                            count: 2,
                            model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                        },
                        GPUResources {
                            count: 1,
                            model: GPU_MODEL_NVIDIA_A10.to_string(),
                        },
                    ],
                },
                expected_can_handle: true,
            },
            Case {
                description: "with gpu - enough resources with exact match of node and host resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu_configs: vec![
                        GPUResources {
                            count: 4,
                            model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                        },
                        GPUResources {
                            count: 2,
                            model: GPU_MODEL_NVIDIA_A10.to_string(),
                        },
                    ],
                },
                expected_can_handle: true,
            },
            Case {
                description: "with gpu - enough resources with exact match of node and host resources and no gpu requested",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu_configs: vec![],
                },
                expected_can_handle: true,
            },
            Case {
                description: "with gpu - not enough count",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }],
                },
                expected_can_handle: false,
            },
            Case {
                description: "with gpu - wrong model",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }],
                },
                expected_can_handle: false,
            },
        ];
        for case in cases {
            assert_eq!(
                case.host_resources
                    .can_handle_function_resources(&case.func_resources)
                    .is_ok(),
                case.expected_can_handle,
                "case: {}",
                case.description
            );
        }
    }

    #[test]
    fn test_consume_function_resources() {
        struct Case {
            description: &'static str,
            host_resources: HostResources,
            func_resources: FunctionResources,
            expected_fe_resources: Option<ContainerResources>, // None means error expected
            expected_host_resources_after: Option<HostResources>, /* None means no change
                                                                * expected */
        }

        let cases = vec![
            Case {
                description: "enough resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_fe_resources: Some(ContainerResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu: None,
                }),
                expected_host_resources_after: Some(HostResources {
                    cpu_ms_per_sec: 1000,
                    memory_bytes: 1024 * 1024 * 1024,
                    disk_bytes: 1024 * 1024 * 1024,
                    gpu: None,
                }),
            },
            Case {
                description: "enough resources with exact match of FE and host resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu_configs: vec![],
                },
                expected_fe_resources: Some(ContainerResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu: None,
                }),
                expected_host_resources_after: Some(HostResources {
                    cpu_ms_per_sec: 0,
                    memory_bytes: 0,
                    disk_bytes: 0,
                    gpu: None,
                }),
            },
            Case {
                description: "not enough cpu",
                host_resources: HostResources {
                    cpu_ms_per_sec: 500,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 501,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_fe_resources: None,         // Error expected
                expected_host_resources_after: None, // No change expected
            },
            Case {
                description: "not enough memory",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 512 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_fe_resources: None,         // Error expected
                expected_host_resources_after: None, // No change expected
            },
            Case {
                description: "not enough disk",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 512 * 1024 * 1024,
                    gpu: None,
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![],
                },
                expected_fe_resources: None,         // Error expected
                expected_host_resources_after: None, // No change expected
            },
            Case {
                description: "with gpu - enough resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![
                        GPUResources {
                            count: 2,
                            model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                        },
                        GPUResources {
                            count: 1,
                            model: GPU_MODEL_NVIDIA_A10.to_string(),
                        },
                    ],
                },
                expected_fe_resources: Some(ContainerResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                }),
                expected_host_resources_after: Some(HostResources {
                    cpu_ms_per_sec: 1000,
                    memory_bytes: 1024 * 1024 * 1024,
                    disk_bytes: 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                }),
            },
            Case {
                description: "with gpu - enough resources with exact match of node and host resources",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu_configs: vec![
                        GPUResources {
                            count: 4,
                            model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                        },
                        GPUResources {
                            count: 2,
                            model: GPU_MODEL_NVIDIA_A10.to_string(),
                        },
                    ],
                },
                expected_fe_resources: Some(ContainerResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                }),
                expected_host_resources_after: Some(HostResources {
                    cpu_ms_per_sec: 0,
                    memory_bytes: 0,
                    disk_bytes: 0,
                    gpu: Some(GPUResources {
                        count: 0,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                }),
            },
            Case {
                description: "with gpu - enough resources with exact match of node and host resources and no gpu requested",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu_configs: vec![],
                },
                expected_fe_resources: Some(ContainerResources {
                    cpu_ms_per_sec: 2000,
                    memory_mb: 2 * 1024,
                    ephemeral_disk_mb: 2 * 1024,
                    gpu: None,
                }),
                expected_host_resources_after: Some(HostResources {
                    cpu_ms_per_sec: 0,
                    memory_bytes: 0,
                    disk_bytes: 0,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                }),
            },
            Case {
                description: "with gpu - not enough count",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }],
                },
                expected_fe_resources: None,         // Error expected
                expected_host_resources_after: None, // No change expected
            },
            Case {
                description: "with gpu - wrong model",
                host_resources: HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 2,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }),
                },
                func_resources: FunctionResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu_configs: vec![GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }],
                },
                expected_fe_resources: None,         // Error expected
                expected_host_resources_after: None, // No change expected
            },
        ];

        for mut case in cases {
            let host_resources_before = case.host_resources.clone();
            let consume_result = case
                .host_resources
                .consume_function_resources(&case.func_resources);
            if let Some(expected_fe_resources) = &case.expected_fe_resources {
                assert!(
                    consume_result.is_ok(),
                    "consume_node_resources should succeed: {}",
                    case.description
                );
                let fe_resources = consume_result.unwrap();
                assert_eq!(
                    fe_resources, *expected_fe_resources,
                    "function executor resources returned from consume_node_resources: {}",
                    case.description
                );

                assert!(
                    case.expected_host_resources_after.is_some(),
                    "expected_host_resources_after should be Some when consume_node_resources succeeds: {}",
                    case.description
                );
                let expected_host_resources = case.expected_host_resources_after.as_ref().unwrap();
                assert_eq!(
                    &case.host_resources, expected_host_resources,
                    "host resources after consume_node_resources: {}",
                    case.description
                );
            } else {
                assert!(
                    consume_result.is_err(),
                    "consume_node_resources should fail: {}",
                    case.description
                );
                assert_eq!(
                    case.host_resources, host_resources_before,
                    "host resources should not change after failed consume_node_resources: {}",
                    case.description
                );
            }
        }
    }

    #[test]
    fn test_free_fe_resources() {
        struct Case {
            description: &'static str,
            host_resources: HostResources,
            fe_resources: ContainerResources,
            expect_success: bool,
            expected_host_resources_after: Option<HostResources>,
        }

        let cases = vec![
            Case {
                description: "free cpu/mem/disk only",
                host_resources: HostResources {
                    cpu_ms_per_sec: 1000,
                    memory_bytes: 1024 * 1024 * 1024,
                    disk_bytes: 1024 * 1024 * 1024,
                    gpu: None,
                },
                fe_resources: ContainerResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu: None,
                },
                expect_success: true,
                expected_host_resources_after: Some(HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: None,
                }),
            },
            Case {
                description: "free with gpu",
                host_resources: HostResources {
                    cpu_ms_per_sec: 1000,
                    memory_bytes: 1024 * 1024 * 1024,
                    disk_bytes: 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                fe_resources: ContainerResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu: Some(GPUResources {
                        count: 5,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                expect_success: true,
                expected_host_resources_after: Some(HostResources {
                    cpu_ms_per_sec: 2000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    disk_bytes: 2 * 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 6,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                }),
            },
            Case {
                description: "free with gpu, wrong model",
                host_resources: HostResources {
                    cpu_ms_per_sec: 1000,
                    memory_bytes: 1024 * 1024 * 1024,
                    disk_bytes: 1024 * 1024 * 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                    }),
                },
                fe_resources: ContainerResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                expect_success: false,
                expected_host_resources_after: None, // No change expected
            },
            Case {
                description: "free with gpu, no gpu on host",
                host_resources: HostResources {
                    cpu_ms_per_sec: 1000,
                    memory_bytes: 1024 * 1024 * 1024,
                    disk_bytes: 1024 * 1024 * 1024,
                    gpu: None,
                },
                fe_resources: ContainerResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu: Some(GPUResources {
                        count: 1,
                        model: GPU_MODEL_NVIDIA_A10.to_string(),
                    }),
                },
                expect_success: false,
                expected_host_resources_after: None, // No change expected
            },
        ];

        for mut case in cases {
            let host_resources_before = case.host_resources.clone();
            let result = case.host_resources.free(&case.fe_resources);
            if case.expect_success {
                assert!(result.is_ok(), "free should succeed: {}", case.description);
                if let Some(expected) = &case.expected_host_resources_after {
                    assert_eq!(
                        &case.host_resources, expected,
                        "host resources after free: {}",
                        case.description
                    );
                } else {
                    assert_eq!(
                        &case.host_resources, &host_resources_before,
                        "host resources after free: {}",
                        case.description
                    );
                }
            } else {
                assert!(result.is_err(), "free should fail: {}", case.description);
            }
        }
    }

    /// Helper to build a Container with given resources for testing.
    fn test_container(resources: ContainerResources) -> crate::data_model::Container {
        ContainerBuilder::default()
            .namespace("test_ns".to_string())
            .application_name("test_app".to_string())
            .function_name("test_fn".to_string())
            .version("1".to_string())
            .state(ContainerState::Pending)
            .resources(resources)
            .max_concurrency(1)
            .container_type(ContainerType::Function)
            .pool_id(ContainerPoolId::new("test_pool"))
            .build()
            .unwrap()
    }

    /// Helper to build an ExecutorServerMetadata with given host resources.
    fn test_executor(host_resources: HostResources) -> ExecutorServerMetadata {
        ExecutorServerMetadataBuilder::default()
            .executor_id(ExecutorId::new("test_executor".to_string()))
            .function_container_ids(HashSet::new())
            .free_resources(host_resources)
            .resource_claims(HashMap::new())
            .build()
            .unwrap()
    }

    /// Regression test for the GPU fallback bug:
    /// When a function lists [A100, H100] and the executor has H100,
    /// consume_function_resources correctly returns H100. Using that
    /// result as the container's resources allows add_container to succeed.
    #[test]
    fn test_add_container_with_gpu_fallback() {
        let host = HostResources {
            cpu_ms_per_sec: 4000,
            memory_bytes: 32 * 1024 * 1024 * 1024,
            disk_bytes: 100 * 1024 * 1024 * 1024,
            gpu: Some(GPUResources {
                count: 1,
                model: GPU_MODEL_NVIDIA_H100_80GB.to_string(),
            }),
        };

        let func_resources = FunctionResources {
            cpu_ms_per_sec: 1000,
            memory_mb: 1024,
            ephemeral_disk_mb: 1024,
            gpu_configs: vec![
                GPUResources {
                    count: 1,
                    model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                },
                GPUResources {
                    count: 1,
                    model: GPU_MODEL_NVIDIA_H100_80GB.to_string(),
                },
            ],
        };

        // Simulate what create_container does after the fix:
        // consume_function_resources picks the matching GPU (H100) and we use
        // its return value as the container's resources.
        let mut host_clone = host.clone();
        let consumed = host_clone
            .consume_function_resources(&func_resources)
            .expect("should find H100 as fallback");

        assert_eq!(
            consumed.gpu,
            Some(GPUResources {
                count: 1,
                model: GPU_MODEL_NVIDIA_H100_80GB.to_string(),
            }),
            "consume_function_resources should pick H100 (second config) since A100 is unavailable"
        );

        // Now use the consumed resources as the container's resources —
        // add_container should succeed because the GPU matches.
        let container = test_container(consumed);
        let mut executor = test_executor(host);
        executor
            .add_container(&container)
            .expect("add_container should succeed with correctly matched H100 GPU");

        // Verify GPU count was decremented
        assert_eq!(
            executor.free_resources.gpu.as_ref().unwrap().count,
            0,
            "H100 GPU count should be decremented to 0"
        );
    }

    /// Verify that consume + add_container works when the preferred GPU (first
    /// in the list) is available on the executor.
    #[test]
    fn test_add_container_with_preferred_gpu() {
        let host = HostResources {
            cpu_ms_per_sec: 4000,
            memory_bytes: 32 * 1024 * 1024 * 1024,
            disk_bytes: 100 * 1024 * 1024 * 1024,
            gpu: Some(GPUResources {
                count: 1,
                model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
            }),
        };

        let func_resources = FunctionResources {
            cpu_ms_per_sec: 1000,
            memory_mb: 1024,
            ephemeral_disk_mb: 1024,
            gpu_configs: vec![
                GPUResources {
                    count: 1,
                    model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                },
                GPUResources {
                    count: 1,
                    model: GPU_MODEL_NVIDIA_H100_80GB.to_string(),
                },
            ],
        };

        let mut host_clone = host.clone();
        let consumed = host_clone
            .consume_function_resources(&func_resources)
            .expect("should find A100 as preferred");

        assert_eq!(
            consumed.gpu.as_ref().unwrap().model,
            GPU_MODEL_NVIDIA_A100_40GB,
            "should pick A100 (first config) since it's available"
        );

        let container = test_container(consumed);
        let mut executor = test_executor(host);
        executor
            .add_container(&container)
            .expect("add_container should succeed with preferred A100 GPU");
    }

    /// Verify that after add_container, freeing the container restores GPU
    /// resources correctly when a fallback GPU was used.
    #[test]
    fn test_add_and_remove_container_with_gpu_fallback() {
        let host = HostResources {
            cpu_ms_per_sec: 4000,
            memory_bytes: 32 * 1024 * 1024 * 1024,
            disk_bytes: 100 * 1024 * 1024 * 1024,
            gpu: Some(GPUResources {
                count: 1,
                model: GPU_MODEL_NVIDIA_H100_80GB.to_string(),
            }),
        };

        let func_resources = FunctionResources {
            cpu_ms_per_sec: 1000,
            memory_mb: 1024,
            ephemeral_disk_mb: 1024,
            gpu_configs: vec![
                GPUResources {
                    count: 1,
                    model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
                },
                GPUResources {
                    count: 1,
                    model: GPU_MODEL_NVIDIA_H100_80GB.to_string(),
                },
            ],
        };

        let mut host_clone = host.clone();
        let consumed = host_clone
            .consume_function_resources(&func_resources)
            .expect("should find H100 as fallback");

        let container = test_container(consumed);
        let mut executor = test_executor(host.clone());

        executor
            .add_container(&container)
            .expect("add_container should succeed");
        assert_eq!(executor.free_resources.gpu.as_ref().unwrap().count, 0);

        executor
            .remove_container(&container)
            .expect("remove_container should succeed");
        assert_eq!(
            executor.free_resources.gpu.as_ref().unwrap().count,
            1,
            "GPU count should be restored after removing container"
        );
        assert_eq!(
            executor.free_resources.gpu.as_ref().unwrap().model,
            GPU_MODEL_NVIDIA_H100_80GB,
            "GPU model should still be H100 after remove"
        );
    }
}
